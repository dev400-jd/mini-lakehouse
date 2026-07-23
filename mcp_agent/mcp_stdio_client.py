"""Minimal hand-rolled MCP client over stdio (newline-delimited JSON-RPC 2.0).

No dependency on the official `mcp` SDK by design (kept out of pyproject.toml
for this hackathon branch) -- the wire protocol used by mcp-trino is a plain
JSON-RPC 2.0 message per line, no Content-Length framing.
"""
import json
import subprocess
import threading


class McpStdioClient:
    def __init__(self, command, env, log_stderr=False):
        self._proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
            bufsize=1,
        )
        self._id = 0
        self._log_stderr = log_stderr
        threading.Thread(target=self._drain_stderr, daemon=True).start()

    def _drain_stderr(self):
        for line in self._proc.stderr:
            if self._log_stderr:
                print("[mcp-server]", line.rstrip())

    def _next_id(self):
        self._id += 1
        return self._id

    def _send(self, msg):
        self._proc.stdin.write(json.dumps(msg) + "\n")
        self._proc.stdin.flush()

    def _recv(self):
        line = self._proc.stdout.readline()
        if not line:
            raise RuntimeError("MCP server closed stdout unexpectedly")
        return json.loads(line)

    def initialize(self, client_name="mcp_agent_harness", client_version="0.1.0"):
        self._send({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": client_name, "version": client_version},
            },
        })
        result = self._recv()
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
        return result.get("result")

    def list_tools(self):
        self._send({"jsonrpc": "2.0", "id": self._next_id(), "method": "tools/list", "params": {}})
        result = self._recv()
        return result["result"]["tools"]

    def call_tool(self, name, arguments):
        self._send({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })
        result = self._recv()
        if "error" in result:
            return {"is_error": True, "text": result["error"].get("message", str(result["error"]))}
        content = result.get("result", {}).get("content", [])
        text = "\n".join(part.get("text", "") for part in content if part.get("type") == "text")
        return {"is_error": result.get("result", {}).get("isError", False), "text": text}

    def close(self):
        try:
            self._proc.terminate()
            self._proc.wait(timeout=5)
        except Exception:
            self._proc.kill()


def mcp_tool_to_ollama_tool(mcp_tool):
    """Convert an MCP tool descriptor to Ollama's OpenAI-style function schema."""
    schema = dict(mcp_tool.get("inputSchema") or {"type": "object"})
    schema.setdefault("properties", {})
    return {
        "type": "function",
        "function": {
            "name": mcp_tool["name"],
            "description": mcp_tool.get("description", ""),
            "parameters": schema,
        },
    }
