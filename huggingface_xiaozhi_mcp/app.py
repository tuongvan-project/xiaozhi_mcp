"""
Trái tim vận hành Cloud 24/7 cho Trợ lý Theo Ly trên HuggingFace Spaces & Render:
1. Cầu nối WebSocket JSON-RPC 2.0 chuẩn MCP kết nối trực tiếp vào `MCP_ENDPOINT` của xiaozhi.me.
2. Tích hợp Cầu chì chống trễ (Circuit Breaker 3.2s) đảm bảo không bao giờ làm chậm hoặc treo loa Xiaozhi.
3. Tự động kết nối lại (Auto-Reconnect) và duy trì nhịp tim (Heartbeat Ping/Pong 20s) 24/24.
4. Hệ thống Kho Nhạc & Audio Đám Mây Độc Lập (Cloud Music Hub): Stream MP3 hỗ trợ HTTP 206 Range Request cho ESP32.
5. Giao diện Web Dashboard Tiếng Việt tại cổng 7860 để quản trị kho nhạc, nghe thử và kiểm thử trực tiếp.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import re
import shutil
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
import websockets
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from cloud_music_hub import AUDIO_DIR, CloudMusicHub
from tools_vn import MCP_TOOLS_SCHEMA, execute_vn_tool, search_youtube_fast

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TheoLyMCP.Bridge")

TOOL_HARD_TIMEOUT_S = float(os.environ.get("TOOL_HARD_TIMEOUT_S", "3.2"))

STATE: dict[str, Any] = {
    "started_at": time.time(),
    "connected": False,
    "endpoint_masked": "Chưa cấu hình MCP_ENDPOINT",
    "reconnect_count": 0,
    "total_calls": 0,
    "last_activity": "Chưa có",
    "last_error": "",
    "recent_logs": deque(maxlen=30),
}

music_hub = CloudMusicHub()


def _get_public_url() -> str:
    return (
        os.environ.get("RENDER_EXTERNAL_URL", "").strip()
        or os.environ.get("PUBLIC_KEEPALIVE_URL", "").strip()
        or ""
    ).rstrip("/")


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

    if msg_id is None:
        if method == "notifications/initialized":
            _log_event("Máy chủ xiaozhi.me đã xác nhận khởi tạo MCP thành công (initialized).")
        return None

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
                    "version": "2.1.0",
                },
            },
        }
        return json.dumps(resp, ensure_ascii=False)

    if method == "ping":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}, ensure_ascii=False)

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

    if method == "resources/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"resources": []}}, ensure_ascii=False)

    if method == "prompts/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"prompts": []}}, ensure_ascii=False)

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
                f"Công cụ '{tool_name}' phản hồi quá {TOOL_HARD_TIMEOUT_S}s nên đã ngắt sớm để giữ ổn định."
            )
            _log_event(f"CẢNH BÁO: '{tool_name}' vượt ngưỡng {elapsed_ms}ms -> Kích hoạt cầu chì.")
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

    return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}, ensure_ascii=False)


async def mcp_websocket_worker(endpoint_url: str) -> None:
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
            _log_event(f"Mất kết nối WebSocket ({e}). Thử lại sau {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def keep_alive_worker() -> None:
    import urllib.request
    await asyncio.sleep(3)
    while True:
        try:
            await asyncio.to_thread(
                execute_vn_tool,
                "vn_daily_life_hub",
                {"aspect": "all", "location": "Tuyên Quang"},
            )
            pub = _get_public_url()
            if pub:
                health_url = pub + "/health"
                await asyncio.to_thread(
                    lambda: urllib.request.urlopen(health_url, timeout=10).read()
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Keep-alive / pre-warm error: {e}")
        await asyncio.sleep(600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    raw_endpoints = os.environ.get("MCP_ENDPOINT", "").strip()
    tasks: list[asyncio.Task] = []
    if raw_endpoints:
        for ep in [x.strip() for x in raw_endpoints.split(",") if x.strip()]:
            tasks.append(asyncio.create_task(mcp_websocket_worker(ep)))
    else:
        _log_event("Chưa tìm thấy biến môi trường MCP_ENDPOINT.")

    tasks.append(asyncio.create_task(keep_alive_worker()))

    yield

    for t in tasks:
        t.cancel()


app = FastAPI(title="Theo Ly - Vietnam Smart MCP & Cloud Music Hub 24/7", lifespan=lifespan)


# ==============================================================================
# HỆ THỐNG PHỤC VỤ ÂM THANH TRỰC TIẾP (DIRECT MP3 + HTTP 206 RANGE REQUEST CHO ESP32)
# ==============================================================================

@app.get("/audio/{file_name}")
async def serve_audio_file(file_name: str, request: Request):
    """
    Phục vụ luồng âm thanh tĩnh chuẩn MP3 hỗ trợ HTTP 206 Range Request.
    ESP32-S3 và I2S Audio Player bắt buộc cần Accept-Ranges: bytes và Content-Range
    để không bị tràn/nghẽn buffer hoặc im lặng.
    """
    safe_name = Path(file_name).name
    file_path = AUDIO_DIR / safe_name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"File âm thanh '{safe_name}' không tồn tại trong kho")

    file_size = file_path.stat().st_size
    range_header = request.headers.get("Range")

    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            start = int(range_match.group(1))
            end_val = range_match.group(2)
            end = int(end_val) if end_val else file_size - 1
            if start >= file_size:
                raise HTTPException(status_code=416, detail="Requested range not satisfiable")
            end = min(end, file_size - 1)
            content_length = end - start + 1

            def iter_chunk():
                with open(file_path, "rb") as f:
                    f.seek(start)
                    rem = content_length
                    while rem > 0:
                        chunk_size = min(rem, 32768)
                        data = f.read(chunk_size)
                        if not data:
                            break
                        rem -= len(data)
                        yield data

            headers = {
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
                "Content-Type": "audio/mpeg",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400",
            }
            return StreamingResponse(iter_chunk(), status_code=206, headers=headers, media_type="audio/mpeg")

    def iter_all():
        with open(file_path, "rb") as f:
            while True:
                data = f.read(32768)
                if not data:
                    break
                yield data

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(file_size),
        "Content-Type": "audio/mpeg",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=86400",
    }
    return StreamingResponse(iter_all(), status_code=200, headers=headers, media_type="audio/mpeg")


# ==============================================================================
# API QUẢN TRỊ KHO NHẠC & AUDIO ĐÁM MÂY (CLOUD AUDIO MANAGEMENT APIS)
# ==============================================================================

@app.get("/api/music/list")
async def api_list_music():
    pub = _get_public_url()
    songs = music_hub.list_songs(public_url=pub)
    return JSONResponse({"count": len(songs), "songs": songs})


@app.post("/api/music/upload")
async def api_upload_music(
    file: UploadFile = File(...),
    title: str = Form(...),
    artist: str = Form(""),
    tags: str = Form(""),
):
    """Tải file MP3 trực tiếp từ trình duyệt lên Kho nhạc Đám mây."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Chưa chọn file")

    safe_name = re.sub(r"[^a-zA-Z0-9_\.-]", "_", file.filename)
    if not safe_name.lower().endswith(".mp3"):
        safe_name += ".mp3"

    dest_path = AUDIO_DIR / safe_name
    try:
        with open(dest_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    finally:
        await file.close()

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    item = music_hub.add_song(
        title=title,
        artist=artist,
        local_file=safe_name,
        tags=tag_list
    )
    _log_event(f"Đã tải lên bài hát mới '{title}' ({safe_name}) vào kho nhạc đám mây.")
    pub = _get_public_url()
    stream_url = f"{pub}/audio/{safe_name}" if pub else f"/audio/{safe_name}"
    return JSONResponse({"status": "ok", "song": item, "stream_url": stream_url})


@app.post("/api/music/add_url")
async def api_add_music_url(
    title: str = Form(...),
    artist: str = Form(""),
    url: str = Form(...),
    tags: str = Form(""),
    duration: str = Form(""),
):
    """Thêm liên kết âm thanh đám mây (Cloudflare R2, Internet Archive, Direct CDN)."""
    clean_url = url.strip()
    if not clean_url.startswith("http"):
        raise HTTPException(status_code=400, detail="URL không hợp lệ")

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    item = music_hub.add_song(
        title=title,
        artist=artist,
        url=clean_url,
        tags=tag_list,
        duration=duration
    )
    _log_event(f"Đã thêm liên kết âm thanh đám mây: '{title}' ({clean_url[:40]}...)")
    return JSONResponse({"status": "ok", "song": item})


@app.post("/api/music/delete")
async def api_delete_music(request: Request):
    data = await request.json()
    song_id = data.get("id", "")
    if not song_id:
        raise HTTPException(status_code=400, detail="Thiếu mã bài hát (id)")
    deleted = music_hub.delete_song(song_id)
    if deleted:
        _log_event(f"Đã xóa bài hát '{song_id}' khỏi kho nhạc đám mây.")
        return JSONResponse({"status": "ok", "deleted": True})
    raise HTTPException(status_code=404, detail="Không tìm thấy bài hát")


@app.get("/api/music/verify")
async def api_verify_music(url: str = Query(...)):
    res = await asyncio.to_thread(music_hub.verify_stream_url, url)
    return JSONResponse(res)


# ==============================================================================
# HỆ THỐNG GIÁM SÁT & ENDPOINT KIỂM THỬ
# ==============================================================================

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
            "cloud_songs_count": len(music_hub.songs),
        }
    )


@app.get("/test-tool")
async def test_tool_api(
    name: str = Query("vn_music_and_culture"),
    arg_json: str = Query('{"song_or_lyric_hint": "Lạc Trôi"}'),
):
    try:
        parsed_args = json.loads(arg_json) if arg_json else {}
    except Exception:
        parsed_args = {}
    t0 = time.perf_counter()
    res = await asyncio.to_thread(execute_vn_tool, name, parsed_args)
    ms = int((time.perf_counter() - t0) * 1000)
    return JSONResponse({"tool": name, "elapsed_ms": ms, "result": res})


@app.get("/search")
async def search_youtube_endpoint(q: str = ""):
    import urllib.parse
    q = urllib.parse.unquote_plus(q or "").strip()
    if not q:
        return JSONResponse({"count": 0, "results": [], "data": {"search": []}})

    # Tra cứu ngay trong kho đám mây trước
    pub = _get_public_url()
    song = music_hub.search_song(q, public_url=pub)
    if song:
        s_url = song.get("stream_url", "")
        title = song.get("title", q)
        return JSONResponse({
            "count": 1,
            "results": [{
                "id": song.get("id"),
                "title": title,
                "duration": song.get("duration", "03:30"),
                "thumbnail": "https://img.icons8.com/color/96/musical-notes.png",
                "stream_url": s_url,
            }],
            "data": {
                "search": [{
                    "episode": {
                        "title": title,
                        "audio": s_url,
                        "duration": song.get("duration", "03:30"),
                        "media": {
                            "content_type": {"description": "audio/mpeg"},
                            "cover": "https://img.icons8.com/color/96/musical-notes.png",
                        },
                    }
                }]
            }
        })

    results = await asyncio.to_thread(search_youtube_fast, q, 3)
    episodes = []
    res_list = []
    for r in results:
        vid = r["id"]
        stream_url = f"{pub}/stream?v={vid}" if pub else f"/stream?v={vid}"
        thumb = r.get("thumbnail", f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg")
        title = r.get("title", "")
        dur = r.get("duration", "")

        res_list.append({"id": vid, "title": title, "duration": dur, "thumbnail": thumb, "stream_url": stream_url})
        episodes.append({
            "episode": {
                "title": title,
                "audio": stream_url,
                "duration": dur,
                "media": {"content_type": {"description": "audio/mp4"}, "cover": thumb},
            }
        })

    return JSONResponse({"count": len(results), "results": res_list, "data": {"search": episodes}})


@app.get("/stream")
async def stream_audio_endpoint(request: Request, q: str = "", v: str = "", url: str = ""):
    q = (q or "").strip()
    pub = _get_public_url()

    # Ưu tiên số 1: Kho nhạc Đám mây cá nhân
    if q:
        song = music_hub.search_song(q, public_url=pub)
        if song and song.get("stream_url"):
            target_url = song["stream_url"]
            # Nếu là URL nội bộ -> redirect hoặc stream
            if target_url.startswith(pub + "/audio/"):
                fname = target_url.split("/audio/")[-1]
                return await serve_audio_file(fname, request)

    # Dự phòng: Radio VOV hoặc SoundHelix test nếu có URL
    if url:
        return StreamingResponse(
            _transcode_stream_to_mp3(url),
            media_type="audio/mpeg",
            headers={"Content-Type": "audio/mpeg", "Accept-Ranges": "bytes", "Access-Control-Allow-Origin": "*"}
        )

    # Nếu không tìm thấy -> Phát bài test đầu tiên trong kho đám mây để tránh câm loa
    songs = music_hub.list_songs(public_url=pub)
    for s in songs:
        if s.get("local_file"):
            return await serve_audio_file(s["local_file"], request)

    raise HTTPException(status_code=404, detail="Không tìm thấy bài hát trong kho đám mây")


def _transcode_stream_to_mp3(source_url: str):
    import subprocess
    import imageio_ffmpeg
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [
        ffmpeg_exe, "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-i", source_url, "-vn", "-acodec", "libmp3lame", "-b:a", "128k", "-f", "mp3", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=32768)
    try:
        while True:
            chunk = proc.stdout.read(16384)
            if not chunk:
                break
            yield chunk
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=1.0)
        except Exception:
            pass


# ==============================================================================
# GIAO DIỆN WEB DASHBOARD & TRUNG TÂM QUẢN TRỊ KHO NHẠC ĐÁM MÂY
# ==============================================================================

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
    pub = _get_public_url()
    song_list = music_hub.list_songs(public_url=pub)

    music_rows = ""
    for s in song_list:
        s_id = s.get("id", "")
        title = s.get("title", "")
        artist = s.get("artist", "")
        dur = s.get("duration", "")
        s_url = s.get("stream_url", "")
        source_badge = (
            '<span style="background:#0284c7;color:#fff;padding:2px 8px;border-radius:6px;font-size:12px;">Đám mây Nội bộ (Direct MP3)</span>'
            if s.get("local_file")
            else '<span style="background:#6366f1;color:#fff;padding:2px 8px;border-radius:6px;font-size:12px;">Cloudflare R2 / CDN</span>'
        )
        audio_player = (
            f'<audio controls preload="none" style="height:32px;width:220px;" src="{s_url}"></audio>'
            if s_url else '<span style="color:#ef4444;">Chưa có luồng</span>'
        )
        del_btn = f'<button onclick="deleteSong(\'{s_id}\')" style="background:#ef4444;color:#fff;border:none;padding:5px 10px;border-radius:6px;cursor:pointer;">Xóa</button>'
        music_rows += f"""
        <tr>
          <td style="padding:10px;border-bottom:1px solid #334155;font-weight:600;color:#f8fafc;">{title}</td>
          <td style="padding:10px;border-bottom:1px solid #334155;color:#94a3b8;">{artist}</td>
          <td style="padding:10px;border-bottom:1px solid #334155;color:#94a3b8;">{dur}</td>
          <td style="padding:10px;border-bottom:1px solid #334155;">{source_badge}</td>
          <td style="padding:10px;border-bottom:1px solid #334155;">{audio_player}</td>
          <td style="padding:10px;border-bottom:1px solid #334155;text-align:center;">{del_btn}</td>
        </tr>
        """

    tools_rows = "".join(
        f"<tr><td style='padding:8px;border-bottom:1px solid #334155;font-weight:600;color:#38bdf8;'>{t['name']}</td>"
        f"<td style='padding:8px;border-bottom:1px solid #334155;'>{t['description']}</td></tr>"
        for t in MCP_TOOLS_SCHEMA
    )

    html_page = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Theo Ly - Vietnam Smart MCP & Cloud Music Hub 24/7</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 24px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }}
  h1 {{ margin-top: 0; font-size: 22px; color: #f1f5f9; }}
  h2 {{ font-size: 18px; color: #38bdf8; margin-top: 0; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; margin-top: 10px; }}
  .log-box {{ font-family: monospace; background: #020617; padding: 14px; border-radius: 8px; font-size: 13px; color: #a7f3d0; max-height: 250px; overflow-y: auto; line-height: 1.6; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 16px; }}
  .stat-item {{ background: #0f172a; padding: 12px; border-radius: 8px; border: 1px solid #334155; }}
  .form-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 14px; }}
  @media (max-width: 768px) {{ .form-grid {{ grid-template-columns: 1fr; }} }}
  .form-box {{ background: #0f172a; padding: 16px; border-radius: 8px; border: 1px solid #334155; }}
  input[type="text"], input[type="file"] {{ width: 100%; box-sizing: border-box; padding: 8px 12px; margin: 6px 0 12px; border-radius: 6px; border: 1px solid #475569; background: #1e293b; color: #fff; }}
  .btn {{ background: #2563eb; color: #fff; border: none; padding: 9px 18px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: 0.2s; }}
  .btn:hover {{ background: #1d4ed8; }}
  .btn-green {{ background: #16a34a; }}
  .btn-green:hover {{ background: #15803d; }}
  .hint-box {{ background: #0c4a6e; border-left: 4px solid #38bdf8; padding: 12px; border-radius: 4px; margin: 12px 0; font-size: 14px; }}
</style>
</head>
<body>
  <div class="card">
    <h1>🤖 Trợ lý Theo Ly — Trung tâm Siêu Công Cụ MCP & Kho Nhạc Đám Mây Cá Nhân 24/7</h1>
    <div style="margin: 14px 0;">{status_badge}</div>
    <div class="stat-grid">
      <div class="stat-item"><b>Điểm kết nối MCP:</b><br>{STATE['endpoint_masked']}</div>
      <div class="stat-item"><b>Thời gian chạy:</b><br>{uptime_min} phút</div>
      <div class="stat-item"><b>Tổng lượt gọi Tool:</b><br>{STATE['total_calls']} lượt</div>
      <div class="stat-item"><b>Số bài hát trên Đám mây:</b><br><span style="color:#38bdf8;font-size:18px;font-weight:700;">{len(song_list)} bài</span></div>
    </div>
  </div>

  <div class="card">
    <h2>🎧 KHO NHẠC & AUDIO ĐÁM MÂY ĐỘC LẬP (KHÔNG LO CHẶN IP - PHÁT TỨC THÌ TRÊN ESP32)</h2>
    <div class="hint-box">
      ✨ <b>Cách sử dụng trên loa ESP32:</b> Bạn chỉ cần nói với loa: <i>"Mở bài Lạc Trôi"</i>, <i>"Bật nhạc thiền 432Hz"</i>, <i>"Nghe giai điệu chuông 528Hz"</i>, <i>"Mở tiếng gió rừng tự nhiên"</i>. Loa sẽ phát ngay trong 0.1 giây từ Đám mây cá nhân chuẩn MP3 HTTP 206 Range Request!
    </div>

    <table>
      <thead>
        <tr style="text-align:left;color:#94a3b8;border-bottom:2px solid #475569;">
          <th style="padding:10px;">Tên Bài Hát</th>
          <th style="padding:10px;">Nghệ Sĩ / Tác Giả</th>
          <th style="padding:10px;">Thời Lượng</th>
          <th style="padding:10px;">Nguồn Phát</th>
          <th style="padding:10px;">Nghe Thử Trực Tiếp</th>
          <th style="padding:10px;text-align:center;">Thao Tác</th>
        </tr>
      </thead>
      <tbody>
        {music_rows}
      </tbody>
    </table>

    <div class="form-grid">
      <!-- Form 1: Upload MP3 -->
      <div class="form-box">
        <h3 style="margin-top:0;font-size:15px;color:#38bdf8;">📥 1. Tải File MP3 Trực Tiếp Lên Đám Mây</h3>
        <form id="uploadForm" onsubmit="handleUpload(event)">
          <label>Tên bài hát (*):</label>
          <input type="text" id="upTitle" required placeholder="Ví dụ: Lạc Trôi">
          <label>Ca sĩ / Nghệ sĩ:</label>
          <input type="text" id="upArtist" placeholder="Ví dụ: Sơn Tùng M-TP">
          <label>Từ khóa gợi ý (phân cách bằng dấu phẩy):</label>
          <input type="text" id="upTags" placeholder="lac troi, son tung, nhac tre">
          <label>Chọn file âm thanh (.mp3):</label>
          <input type="file" id="upFile" accept=".mp3" required>
          <button type="submit" class="btn btn-green">📤 Tải Lên Đám Mây & Lưu Vào Danh Mục</button>
        </form>
      </div>

      <!-- Form 2: Thêm URL ngoài -->
      <div class="form-box">
        <h3 style="margin-top:0;font-size:15px;color:#38bdf8;">🌐 2. Thêm Liên Kết Đám Mây Ngoài (Cloudflare R2 / CDN / Internet Archive)</h3>
        <form id="urlForm" onsubmit="handleAddUrl(event)">
          <label>Tên bài hát (*):</label>
          <input type="text" id="urlTitle" required placeholder="Ví dụ: Tìm Lại Bầu Trời">
          <label>Ca sĩ / Nghệ sĩ:</label>
          <input type="text" id="urlArtist" placeholder="Ví dụ: Tuấn Hưng">
          <label>Đường dẫn MP3 trực tiếp (Direct Audio URL):</label>
          <input type="text" id="urlInput" required placeholder="https://pub-xxxx.r2.dev/song.mp3 hoặc Internet Archive">
          <label>Từ khóa gợi ý (Tags):</label>
          <input type="text" id="urlTags" placeholder="tim lai bau troi, tuan hung">
          <button type="button" class="btn" onclick="verifyUrl()" style="background:#475569;margin-right:8px;">🔍 Kiểm Tra Kỹ Thuật (ESP32 Ready)</button>
          <button type="submit" class="btn">💾 Lưu Vào Kho Đám Mây</button>
          <div id="verifyResult" style="margin-top:8px;font-size:13px;"></div>
        </form>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>🛠️ 6 Siêu Công Cụ (Super-Tools) MCP đang vận hành cho Loa</h2>
    <table>{tools_rows}</table>
  </div>

  <div class="card">
    <h2>📜 Nhật Ký Hoạt Động Thời Gian Thực (Live Logs)</h2>
    <div class="log-box">{logs_html}</div>
  </div>

  <script>
    async function handleUpload(e) {{
      e.preventDefault();
      const fd = new FormData();
      fd.append('title', document.getElementById('upTitle').value);
      fd.append('artist', document.getElementById('upArtist').value);
      fd.append('tags', document.getElementById('upTags').value);
      fd.append('file', document.getElementById('upFile').files[0]);
      
      const res = await fetch('/api/music/upload', {{ method: 'POST', body: fd }});
      const data = await res.json();
      if (res.ok) {{
        alert('Tải lên thành công: ' + data.song.title);
        location.reload();
      }} else {{
        alert('Lỗi: ' + (data.detail || 'Không thể tải lên'));
      }}
    }}

    async function handleAddUrl(e) {{
      e.preventDefault();
      const fd = new FormData();
      fd.append('title', document.getElementById('urlTitle').value);
      fd.append('artist', document.getElementById('urlArtist').value);
      fd.append('url', document.getElementById('urlInput').value);
      fd.append('tags', document.getElementById('urlTags').value);

      const res = await fetch('/api/music/add_url', {{ method: 'POST', body: fd }});
      const data = await res.json();
      if (res.ok) {{
        alert('Thêm liên kết đám mây thành công!');
        location.reload();
      }} else {{
        alert('Lỗi: ' + (data.detail || 'Không thể thêm URL'));
      }}
    }}

    async function verifyUrl() {{
      const u = document.getElementById('urlInput').value.trim();
      const box = document.getElementById('verifyResult');
      if (!u) {{ alert('Vui lòng nhập URL'); return; }}
      box.innerHTML = '⏳ Đang kiểm định chuẩn HTTP Range Request cho ESP32...';
      try {{
        const res = await fetch('/api/music/verify?url=' + encodeURIComponent(u));
        const d = await res.json();
        if (d.valid) {{
          box.innerHTML = `<span style="color:#4ade80;">✔ Chuẩn ESP32! TTFB: ${{d.ttfb_ms}}ms, HTTP ${{d.status_code}}, Range Request ${{d.accept_ranges || 'OK'}}</span>`;
        }} else {{
          box.innerHTML = `<span style="color:#ef4444;">✖ Chưa tương thích: ${{d.error || 'Thiếu hỗ trợ byte range'}} (TTFB: ${{d.ttfb_ms}}ms)</span>`;
        }}
      }} catch (err) {{
        box.innerHTML = '<span style="color:#ef4444;">Lỗi kết nối kiểm tra</span>';
      }}
    }}

    async function deleteSong(id) {{
      if (!confirm('Bạn có chắc muốn xóa bài hát này khỏi kho đám mây?')) return;
      const res = await fetch('/api/music/delete', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ id: id }})
      }});
      if (res.ok) {{
        location.reload();
      }} else {{
        alert('Không thể xóa bài hát');
      }}
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(html_page)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
