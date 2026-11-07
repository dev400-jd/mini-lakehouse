"""DS2 eval harness: run the ground-truth question set against Ollama + the
Trino MCP server, for one or more models, and record what happened.

Bypasses the ollmcp TUI entirely (its interactive HIL keypress listener does
not play well with non-interactive automation) and talks to the MCP server
and Ollama directly. Automated execution is safe here because:
  - the question set is read-only by design
  - mcp-trino enforces read-only at the SQL level regardless of what the
    model tries (see docs/... TRINO_ALLOW_WRITE_QUERIES, left unset)
  - this is a local sandbox run, not a live/production agent session

Usage:
    uv run python mcp_agent/eval_harness.py
    uv run python mcp_agent/eval_harness.py --models qwen2.5:7b --limit 3
"""
import argparse
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from mcp_stdio_client import McpStdioClient, mcp_tool_to_ollama_tool

HERE = Path(__file__).resolve().parent

SYSTEM_PROMPT = (
    "Du bist ein Assistent mit Zugriff auf ein Trino-Lakehouse ueber MCP-Tools. "
    "Nutze die verfuegbaren Tools, um Fragen zu Katalogen, Schemas, Tabellen und "
    "Daten zu beantworten. Antworte praezise auf Basis der Tool-Ergebnisse. "
    "Wenn eine angefragte Ressource nicht existiert, sage das explizit, statt "
    "eine Antwort zu erfinden."
)

NUDGE_MESSAGE = (
    "Das war kein echter Tool-Aufruf. Bitte rufe das Tool jetzt tatsaechlich auf."
)


def resolve_mcp_trino_bin():
    env_bin = os.environ.get("MCP_TRINO_BIN")
    if env_bin:
        return env_bin
    found = shutil.which("mcp-trino")
    if found:
        return found
    fallback = str(Path.home() / ".local" / "bin" / "mcp-trino")
    if Path(fallback).exists():
        return fallback
    raise RuntimeError(
        "mcp-trino binary not found. Install it (see mcp_agent/README.md) or "
        "set MCP_TRINO_BIN to its path."
    )


def mcp_trino_env():
    env = os.environ.copy()
    env.update({
        "TRINO_HOST": "localhost",
        "TRINO_PORT": "8080",
        "TRINO_USER": "mcp_agent",
        "TRINO_CATALOG": "nessie",
        "TRINO_SCHEMA": "trusted",
        "TRINO_SSL": "false",
        "TRINO_SCHEME": "http",
        "TRINO_MAX_ROWS": "1000",
        "TRINO_QUERY_TIMEOUT": "30",
    })
    return env


def looks_like_fake_tool_call(content, tool_names):
    if not content:
        return False
    lowered = content.lower()
    if '"name"' in lowered and ('"arguments"' in lowered or '"function"' in lowered):
        return True
    return any(name in lowered for name in tool_names)


def run_question(host, model, tools, tool_names, mcp_client, question, max_turns, max_nudges):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question["question"]},
    ]
    trace = []
    nudges_used = 0
    start = time.time()
    final_answer = None
    hit_max_turns = False

    for turn in range(max_turns):
        resp = requests.post(
            f"{host}/api/chat",
            json={"model": model, "messages": messages, "tools": tools, "stream": False},
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()
        msg = data["message"]
        messages.append(msg)

        tool_calls = msg.get("tool_calls") or []
        if tool_calls:
            for tc in tool_calls:
                fn = tc["function"]
                name = fn["name"]
                args = fn.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except json.JSONDecodeError:
                        args = {}
                result = mcp_client.call_tool(name, args)
                trace.append({"turn": turn, "tool": name, "arguments": args, "result": result})
                messages.append({"role": "tool", "content": result["text"]})
            continue

        content = msg.get("content", "") or ""
        if looks_like_fake_tool_call(content, tool_names) and nudges_used < max_nudges:
            nudges_used += 1
            trace.append({"turn": turn, "tool": None, "note": "fake_tool_call_narration_detected"})
            messages.append({"role": "user", "content": NUDGE_MESSAGE})
            continue

        final_answer = content
        break
    else:
        hit_max_turns = True
        final_answer = messages[-1].get("content", "") if messages else ""

    elapsed = time.time() - start
    final_answer = final_answer or ""
    check_strings = question["check_strings"]
    check_mode = question.get("check_mode", "all")

    def check(text):
        lowered = (text or "").lower()
        matched = [s for s in check_strings if s.lower() in lowered]
        if check_mode == "any":
            verdict = "pass" if matched else "fail"
        else:
            verdict = "pass" if len(matched) == len(check_strings) else (
                "partial" if matched else "fail"
            )
        return verdict, matched

    answer_check, matched_in_answer = check(final_answer)

    # grounded_check looks only at real tool RESULT text (what Trino actually
    # returned), not the model's prose -- catches cases where the final
    # answer happens to contain a check string by luck/hallucination without
    # ever having fetched the right data (see e.g. Q14 qwen2.5:1.5b).
    grounded_text = "\n".join(t["result"]["text"] for t in trace if t.get("tool") and t.get("result"))
    if check_mode == "any":
        grounded_check, matched_in_trace = "n/a", []
        auto_check = answer_check
    else:
        grounded_check, matched_in_trace = check(grounded_text)
        if grounded_check == "pass" and answer_check == "pass":
            auto_check = "pass"
        elif grounded_check != "pass":
            auto_check = "no_evidence"
        else:
            auto_check = "partial_mismatch"

    return {
        "id": question["id"],
        "category": question["category"],
        "question": question["question"],
        "ground_truth": question["ground_truth"],
        "model": model,
        "final_answer": final_answer,
        "tool_calls_count": sum(1 for t in trace if t.get("tool")),
        "nudges_used": nudges_used,
        "hit_max_turns": hit_max_turns,
        "elapsed_seconds": round(elapsed, 1),
        "check_strings": check_strings,
        "check_mode": check_mode,
        "matched_check_strings": matched_in_answer,
        "answer_check": answer_check,
        "grounded_check": grounded_check,
        "matched_in_trace": matched_in_trace,
        "auto_check": auto_check,
        "trace": trace,
    }


def write_markdown_summary(results, out_path, repeats):
    lines = [
        "# DS2 Eval Harness Results",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Repeats per (model, question): {repeats}",
        "",
        "Note: `auto_check` is a crude substring heuristic against `check_strings`, "
        "not a real correctness grade. Always cross-check `final_answer` against "
        "`ground_truth` by hand. `pass_rate` is passes/repeats across independent "
        "runs of the same question -- Ollama's default sampling is not "
        "deterministic, so a single run is not a reliable comparison. "
        "`no_evidence` = the real tool-result text never contained the expected "
        "value (no grounding, regardless of what the model's prose claimed); "
        "`partial_mismatch` = the right data WAS fetched via tools but the final "
        "answer failed to report it correctly.",
        "",
    ]

    if repeats > 1:
        groups = {}
        for r in results:
            groups.setdefault((r["model"], r["id"]), []).append(r)
        lines += [
            "| id | category | model | pass_rate | no_evidence | partial_mismatch | avg tools | avg time (s) | question |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for (model, qid), items in sorted(groups.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            passes = sum(1 for i in items if i["auto_check"] == "pass")
            no_evidence = sum(1 for i in items if i["auto_check"] == "no_evidence")
            partial_mismatch = sum(1 for i in items if i["auto_check"] == "partial_mismatch")
            avg_tools = sum(i["tool_calls_count"] for i in items) / len(items)
            avg_time = sum(i["elapsed_seconds"] for i in items) / len(items)
            lines.append(
                f"| {qid} | {items[0]['category']} | {model} | {passes}/{len(items)} | "
                f"{no_evidence}/{len(items)} | {partial_mismatch}/{len(items)} | "
                f"{avg_tools:.1f} | {avg_time:.1f} | {items[0]['question'][:60]} |"
            )
    else:
        lines += [
            "| id | category | model | auto_check | tools | nudges | max_turns_hit | time (s) | question |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for r in results:
            lines.append(
                f"| {r['id']} | {r['category']} | {r['model']} | {r['auto_check']} | "
                f"{r['tool_calls_count']} | {r['nudges_used']} | {r['hit_max_turns']} | "
                f"{r['elapsed_seconds']} | {r['question'][:60]} |"
            )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=["qwen2.5:7b", "qwen2.5:1.5b"])
    parser.add_argument("--questions", default=str(HERE / "questions.json"))
    parser.add_argument("--out-dir", default=str(HERE / "results"))
    parser.add_argument("--host", default="http://localhost:11434")
    parser.add_argument("--max-turns", type=int, default=8)
    parser.add_argument("--max-nudges", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None, help="Only run the first N questions")
    parser.add_argument("--repeats", type=int, default=1,
                         help="Run each (model, question) this many times, independently")
    args = parser.parse_args()

    questions = json.loads(Path(args.questions).read_text(encoding="utf-8"))
    if args.limit:
        questions = questions[: args.limit]

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    mcp_client = McpStdioClient([resolve_mcp_trino_bin()], env=mcp_trino_env())
    mcp_client.initialize()
    mcp_tools = mcp_client.list_tools()
    tools = [mcp_tool_to_ollama_tool(t) for t in mcp_tools]
    tool_names = [t["name"] for t in mcp_tools]
    print(f"Connected to MCP server, {len(mcp_tools)} tool(s): {', '.join(tool_names)}")

    all_results = []
    try:
        for model in args.models:
            print(f"\n=== Model: {model} ===")
            for q in questions:
                for rep in range(args.repeats):
                    suffix = f" (repeat {rep + 1}/{args.repeats})" if args.repeats > 1 else ""
                    print(f"  [{q['id']}] {q['question'][:60]}...{suffix}")
                    result = run_question(
                        args.host, model, tools, tool_names, mcp_client, q,
                        args.max_turns, args.max_nudges,
                    )
                    result["repeat"] = rep
                    print(f"      -> auto_check={result['auto_check']} "
                          f"tools={result['tool_calls_count']} nudges={result['nudges_used']} "
                          f"time={result['elapsed_seconds']}s")
                    all_results.append(result)
    finally:
        mcp_client.close()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"results_{timestamp}.json"
    md_path = out_dir / f"results_{timestamp}.md"
    json_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")
    write_markdown_summary(all_results, md_path, args.repeats)
    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
