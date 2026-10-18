"""
Trái tim vận hành Cloud 24/7 cho Trợ lý Theo Ly trên HuggingFace Spaces:
1. Cầu nối WebSocket JSON-RPC 2.0 chuẩn MCP kết nối trực tiếp vào `MCP_ENDPOINT` của xiaozhi.me.
2. Tích hợp Cầu chì chống trễ (Circuit Breaker 3.2s) đảm bảo không bao giờ làm chậm hoặc treo loa Xiaozhi.
3. Tự động kết nối lại (Auto-Reconnect) và duy trì nhịp tim (Heartbeat Ping/Pong 20s) 24/24.
4. Giao diện Web Dashboard Tiếng Việt tại cổng 7860 để giám sát và kiểm thử trực tiếp.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
import websockets
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

from tools_vn import MCP_TOOLS_SCHEMA, execute_vn_tool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TheoLyMCP.Bridge")

# Cấu hình giới hạn thời gian tối đa cho 1 lần gọi Tool (giây) để bảo vệ xiaozhi.me
TOOL_HARD_TIMEOUT_S = float(os.environ.get("TOOL_HARD_TIMEOUT_S", "3.2"))

STATE: dict[str, Any] = {
    "started_at": time.time(),
    "connected": False,
    "endpoint_masked": "Chưa cấu hình MCP_ENDPOINT",
    "reconnect_count": 0,
    "total_calls": 0,
    "last_activity": "Chưa có",
    "last_error": "",
    "recent_logs": deque(maxlen=25),
}


def _mask_endpoint(url: str) -> str:
    if not url:
        return "Chưa cấu hình MCP_ENDPOINT"
    if "token=" in url:
        base, tok = url.split("token=", 1)
        masked_tok = (tok[:6] + "..." + tok[-4:]) if len(tok) > 12 else "***"
        return f"{base}token={masked_tok}"
    return url[:35] + "..."


def _log_event(msg: str) -> None:
    vn_tz = datetime.timezone(datetime.timedelta(hours=7))
    ts = datetime.datetime.now(vn_tz).strftime("%H:%M:%S %d/%m")
    entry = f"[{ts}] {msg}"
    STATE["recent_logs"].appendleft(entry)
    STATE["last_activity"] = ts
    logger.info(msg)


async def handle_mcp_jsonrpc(raw_msg: str) -> str | None:
    """
    Xử lý chuẩn xác giao thức MCP JSON-RPC 2.0 từ máy chủ xiaozhi.me.
    Trả về chuỗi JSON phản hồi (hoặc None nếu là notification không cần phản hồi).
    """
    try:
        req = json.loads(raw_msg)
    except Exception as e:
        logger.warning(f"Bỏ qua gói tin không phải JSON: {e}")
        return None

    if not isinstance(req, dict):
        return None

    method = req.get("method", "")
    msg_id = req.get("id")
    params = req.get("params") or {}

    # 1. Các gói Notification (không có id) -> Không trả lời
    if msg_id is None:
        if method == "notifications/initialized":
            _log_event("Máy chủ xiaozhi.me đã xác nhận khởi tạo MCP thành công (initialized).")
        return None

    # 2. Khởi tạo phiên làm việc MCP (initialize)
    if method == "initialize":
        client_proto = params.get("protocolVersion", "2024-11-05")
        _log_event(f"Nhận yêu cầu initialize từ xiaozhi.me (protocol={client_proto}).")
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": client_proto,
                "capabilities": {
                    "tools": {"listChanged": False},
                },
                "serverInfo": {
                    "name": "TheoLy-Vietnam-SmartMCP",
                    "version": "2.0.0",
                },
            },
        }
        return json.dumps(resp, ensure_ascii=False)

    # 3. Kiểm tra nhịp tim (ping)
    if method == "ping":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}, ensure_ascii=False)

    # 4. Liệt kê danh sách công cụ (tools/list)
    if method == "tools/list":
        _log_event(f"Gửi danh sách {len(MCP_TOOLS_SCHEMA)} Siêu Công Cụ Tiếng Việt lên xiaozhi.me.")
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "tools": MCP_TOOLS_SCHEMA,
            },
        }
        return json.dumps(resp, ensure_ascii=False)

    # 5. Danh sách tài nguyên / prompt rỗng để tương thích 100% chuẩn MCP
    if method == "resources/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"resources": []}}, ensure_ascii=False)

    if method == "prompts/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"prompts": []}}, ensure_ascii=False)

    # 6. Thực thi công cụ (tools/call) kèm Cầu chì chống trễ 3.2s
    if method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments") or {}
        t0 = time.perf_counter()
        STATE["total_calls"] += 1

        try:
            result_text = await asyncio.wait_for(
                asyncio.to_thread(execute_vn_tool, tool_name, tool_args),
                timeout=TOOL_HARD_TIMEOUT_S,
            )
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            _log_event(f"Gọi công cụ '{tool_name}' ({tool_args}) -> Xong trong {elapsed_ms}ms.")
            resp = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": result_text}],
                    "isError": False,
                },
            }
            return json.dumps(resp, ensure_ascii=False)
        except asyncio.TimeoutError:
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            fallback_msg = (
                f"Công cụ '{tool_name}' phản hồi quá {TOOL_HARD_TIMEOUT_S}s nên đã ngắt sớm để giữ ổn định. "
                f"Hãy trả lời ngắn gọn dựa trên kiến thức sẵn có."
            )
            _log_event(f"CẢNH BÁO: '{tool_name}' vượt ngưỡng {elapsed_ms}ms -> Kích hoạt cầu chì bảo vệ.")
            resp = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": fallback_msg}],
                    "isError": False,
                },
            }
            return json.dumps(resp, ensure_ascii=False)
        except Exception as e:
            _log_event(f"Lỗi thực thi '{tool_name}': {e}")
            resp = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": f"Lỗi tạm thời: {e}"}],
                    "isError": True,
                },
            }
            return json.dumps(resp, ensure_ascii=False)

    # Phương thức không xác định -> Trả về kết quả rỗng an toàn
    return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}, ensure_ascii=False)


async def mcp_websocket_worker(endpoint_url: str) -> None:
    """Tiến trình ngầm giữ kết nối WebSocket 24/7 với máy chủ xiaozhi.me."""
    backoff = 2
    STATE["endpoint_masked"] = _mask_endpoint(endpoint_url)

    while True:
        try:
            _log_event(f"Đang kết nối tới xiaozhi.me ({STATE['endpoint_masked']})...")
            async with websockets.connect(
                endpoint_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2 * 1024 * 1024,
            ) as ws:
                STATE["connected"] = True
                STATE["last_error"] = ""
                backoff = 2
                _log_event("ĐÃ KẾT NỐI THÀNH CÔNG với máy chủ xiaozhi.me! Sẵn sàng phục vụ 24/7.")

                async for message in ws:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8", errors="ignore")
                    reply = await handle_mcp_jsonrpc(message)
                    if reply:
                        await ws.send(reply)

        except asyncio.CancelledError:
            STATE["connected"] = False
            break
        except Exception as e:
            STATE["connected"] = False
            STATE["reconnect_count"] += 1
            STATE["last_error"] = str(e)
            _log_event(f"Mất kết nối WebSocket ({e}). Tự động kết nối lại sau {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def keep_alive_worker() -> None:
    """
    1. Làm nóng sẵn bộ nhớ đệm (Pre-warm Cache) cho Thời tiết Tuyên Quang + Giá vàng SJC/Xăng dầu
       để khi người dùng hỏi trên loa, thời gian phản hồi đạt 0ms (tức thì).
    2. Tự động gửi HTTP GET vào chính URL công khai (RENDER_EXTERNAL_URL/health) mỗi 10 phút
       để giữ cho gói Free của Render.com thức 24/7.
    """
    import urllib.request

    await asyncio.sleep(3)
    while True:
        try:
            # Làm nóng cache ngầm để tốc độ phản hồi trên loa đạt 0-2ms
            await asyncio.to_thread(
                execute_vn_tool,
                "vn_daily_life_hub",
                {"aspect": "all", "location": "Tuyên Quang"},
            )
            public_url = (
                os.environ.get("RENDER_EXTERNAL_URL", "").strip()
                or os.environ.get("PUBLIC_KEEPALIVE_URL", "").strip()
            )
            if public_url:
                health_url = public_url.rstrip("/") + "/health"
                await asyncio.to_thread(
                    lambda: urllib.request.urlopen(health_url, timeout=10).read()
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Keep-alive / pre-warm error: {e}")
        await asyncio.sleep(600)  # 10 phút cập nhật ngầm & ping 1 lần


@asynccontextmanager
async def lifespan(app: FastAPI):
    raw_endpoints = os.environ.get("MCP_ENDPOINT", "").strip()
    tasks: list[asyncio.Task] = []
    if raw_endpoints:
        for ep in [x.strip() for x in raw_endpoints.split(",") if x.strip()]:
            tasks.append(asyncio.create_task(mcp_websocket_worker(ep)))
    else:
        _log_event("Chưa tìm thấy biến môi trường MCP_ENDPOINT. Hãy thêm MCP_ENDPOINT trong phần Environment Variables.")

    tasks.append(asyncio.create_task(keep_alive_worker()))

    yield

    for t in tasks:
        t.cancel()


app = FastAPI(title="Theo Ly - Vietnam Smart MCP Cloud 24/7", lifespan=lifespan)


@app.get("/health")
async def health_check():
    uptime_s = int(time.time() - STATE["started_at"])
    return JSONResponse(
        {
            "status": "ok",
            "connected_to_xiaozhi": STATE["connected"],
            "uptime_seconds": uptime_s,
            "total_tool_calls": STATE["total_calls"],
            "tools_count": len(MCP_TOOLS_SCHEMA),
        }
    )


@app.get("/test-tool")
async def test_tool_api(
    name: str = Query("vn_daily_life_hub"),
    arg_json: str = Query('{"aspect": "all", "location": "Tuyên Quang"}'),
):
    try:
        parsed_args = json.loads(arg_json) if arg_json else {}
    except Exception:
        parsed_args = {}
    t0 = time.perf_counter()
    res = await asyncio.to_thread(execute_vn_tool, name, parsed_args)
    ms = int((time.perf_counter() - t0) * 1000)
    return JSONResponse({"tool": name, "elapsed_ms": ms, "result": res})


@app.get("/", response_class=HTMLResponse)
async def dashboard_ui():
    uptime_min = int((time.time() - STATE["started_at"]) // 60)
    status_badge = (
        '<span style="background:#16a34a;color:#fff;padding:6px 14px;border-radius:99px;font-weight:700;">'
        '🟢 ĐÃ KẾT NỐI XIAOZHI.ME (ONLINE 24/7)</span>'
        if STATE["connected"]
        else '<span style="background:#dc2626;color:#fff;padding:6px 14px;border-radius:99px;font-weight:700;">'
        '🔴 CHƯA KẾT NỐI (Hãy kiểm tra biến MCP_ENDPOINT)</span>'
    )
    logs_html = "<br>".join(STATE["recent_logs"]) or "Chưa có nhật ký."
    tools_rows = "".join(
        f"<tr><td style='padding:8px;border-bottom:1px solid #334155;font-weight:600;color:#38bdf8;'>{t['name']}</td>"
        f"<td style='padding:8px;border-bottom:1px solid #334155;'>{t['description']}</td></tr>"
        for t in MCP_TOOLS_SCHEMA
    )
    html_page = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>Theo Ly - Vietnam Smart MCP Cloud 24/7</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 24px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }}
  h1 {{ margin-top: 0; font-size: 22px; color: #f1f5f9; }}
  h2 {{ font-size: 17px; color: #94a3b8; margin-top: 0; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  .log-box {{ font-family: monospace; background: #020617; padding: 14px; border-radius: 8px; font-size: 13px; color: #a7f3d0; max-height: 280px; overflow-y: auto; line-height: 1.6; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 16px; }}
  .stat-item {{ background: #0f172a; padding: 12px; border-radius: 8px; border: 1px solid #334155; }}
</style>
</head>
<body>
  <div class="card">
    <h1>🤖 Trợ lý Theo Ly — Trung tâm Siêu Công Cụ MCP Việt Nam (Cloud 24/7)</h1>
    <div style="margin: 14px 0;">{status_badge}</div>
    <div class="stat-grid">
      <div class="stat-item"><b>Điểm kết nối:</b><br>{STATE['endpoint_masked']}</div>
      <div class="stat-item"><b>Thời gian chạy:</b><br>{uptime_min} phút</div>
      <div class="stat-item"><b>Tổng lượt gọi Tool:</b><br>{STATE['total_calls']} lượt</div>
      <div class="stat-item"><b>Cầu chì chống trễ:</b><br>{TOOL_HARD_TIMEOUT_S} giây</div>
    </div>
  </div>
  <div class="card">
    <h2>🛠️ 6 Siêu Công Cụ (Super-Tools) đang trang bị cho Theo Ly</h2>
    <table>{tools_rows}</table>
  </div>
  <div class="card">
    <h2>📜 Nhật ký hoạt động thời gian thực</h2>
    <div class="log-box">{logs_html}</div>
  </div>
</body>
</html>"""
    return HTMLResponse(html_page)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
