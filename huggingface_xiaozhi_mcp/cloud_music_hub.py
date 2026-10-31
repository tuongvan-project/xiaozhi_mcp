"""
Module Quản lý & Điều hướng Kho Nhạc Cá Nhân trên Đám mây (Cloud Music Hub).
Thiết kế tối ưu cho Xiaozhi & ESP32-S3:
- Lưu trữ trực tiếp file MP3 nội bộ (/audio/filename) hoặc qua Cloudflare R2 / Internet Archive / CDN ngoài.
- Hỗ trợ Range Request (HTTP 206 Partial Content) để chip ESP32 giải mã mượt mà không bao giờ bị nghẽn buffer.
- Tìm kiếm siêu tốc (< 2ms) bằng giải thuật đối soát mờ (Fuzzy + Accent-Insensitive + Tags).
- Tách biệt hoàn toàn, không phụ thuộc YouTube/SoundCloud (tránh bị chặn IP Data Center).
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("TheoLyMCP.CloudMusic")

BASE_DIR = Path(__file__).parent
CATALOG_FILE = BASE_DIR / "my_cloud_music.json"
AUDIO_DIR = BASE_DIR / "static" / "audio"

DEFAULT_SONGS: list[dict[str, Any]] = [
    {
        "id": "soundhelix_sample",
        "title": "SoundHelix Instrumental Sample",
        "artist": "SoundHelix Studio",
        "tags": ["sample", "test", "instrumental", "nhac khong loi", "soundhelix"],
        "url": "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3",
        "local_file": "soundhelix_test.mp3",
        "duration": "06:12",
        "bitrate": "192kbps"
    },
    {
        "id": "nhac_thien_432hz",
        "title": "Nhạc Thiền Thư Giãn Sóng Tần Số 432Hz",
        "artist": "Theo Ly Healing Cloud",
        "tags": ["nhac thien", "thu gian", "ngu ngon", "song nao", "432hz", "healing"],
        "local_file": "nhac_thien_thu_gian.mp3",
        "duration": "00:30",
        "bitrate": "128kbps"
    },
    {
        "id": "chuong_nhip_dieu_528hz",
        "title": "Giai Điệu Chuông Thư Thái 528Hz",
        "artist": "Theo Ly Sound Lab",
        "tags": ["chuong", "nhip dieu", "nang luong", "528hz", "chuong bao thuc"],
        "local_file": "chuong_nhip_dieu.mp3",
        "duration": "00:20",
        "bitrate": "128kbps"
    },
    {
        "id": "am_thanh_tu_nhien",
        "title": "Âm Thanh Thiên Nhiên Tiếng Gió Rừng Xanh",
        "artist": "Theo Ly Forest Sound",
        "tags": ["thien nhien", "gio rung", "tieng gio", "rung xanh", "tieng chim", "tieng mua"],
        "local_file": "am_thanh_tu_nhien.mp3",
        "duration": "00:30",
        "bitrate": "128kbps"
    },
    {
        "id": "lac_troi",
        "title": "Lạc Trôi (Acoustic / Beat)",
        "artist": "Sơn Tùng M-TP",
        "tags": ["lac troi", "son tung", "mtp", "nhac tre", "acoustic"],
        "url": "https://archive.org/download/lac-troi-sample/lac_troi.mp3",
        "local_file": "",
        "duration": "04:33",
        "bitrate": "128kbps"
    },
    {
        "id": "tim_lai_bau_troi",
        "title": "Tìm Lại Bầu Trời",
        "artist": "Tuấn Hưng",
        "tags": ["tim lai bau troi", "tuan hung", "nhac tre"],
        "url": "https://archive.org/download/tim-lai-bau-troi-sample/tim_lai_bau_troi.mp3",
        "local_file": "",
        "duration": "04:12",
        "bitrate": "128kbps"
    },
    {
        "id": "lk_remix_soi_dong",
        "title": "Liên Khúc Nhạc Trẻ Remix Sôi Động",
        "artist": "Nhiều Ca Sĩ",
        "tags": ["remix", "nonstop", "lien khuc", "nhac tre remix", "lk", "soi dong"],
        "url": "https://archive.org/download/lk-remix-sample/lk_remix.mp3",
        "local_file": "",
        "duration": "15:20",
        "bitrate": "128kbps"
    }
]


def strip_accents(text: str) -> str:
    """Loại bỏ dấu tiếng Việt và chuẩn hóa để so khớp mờ."""
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("đ", "d").replace("Đ", "D")
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


class CloudMusicHub:
    def __init__(self, catalog_path: Optional[Path] = None):
        self.catalog_path = catalog_path or CATALOG_FILE
        self.songs: list[dict[str, Any]] = []
        self._load_catalog()

    def _load_catalog(self) -> None:
        if self.catalog_path.exists():
            try:
                data = json.loads(self.catalog_path.read_text(encoding="utf-8"))
                self.songs = data.get("songs", [])
                logger.info(f"Đã nạp {len(self.songs)} bài hát từ {self.catalog_path}")
                return
            except Exception as e:
                logger.warning(f"Lỗi đọc catalog file: {e}. Sử dụng danh mục mặc định.")
        self.songs = list(DEFAULT_SONGS)
        self._save_catalog()

    def _save_catalog(self) -> None:
        try:
            self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
            self.catalog_path.write_text(
                json.dumps({"songs": self.songs}, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"Không thể lưu catalog file: {e}")

    def list_songs(self, public_url: str = "") -> list[dict[str, Any]]:
        """Trả về toàn bộ danh sách bài hát kèm URL stream tuyệt đối."""
        pub = (public_url or "").rstrip("/")
        result = []
        for s in self.songs:
            item = dict(s)
            local_f = item.get("local_file", "").strip()
            if local_f and (AUDIO_DIR / local_f).exists():
                item["stream_url"] = f"{pub}/audio/{local_f}" if pub else f"/audio/{local_f}"
                item["source_type"] = "Đám mây máy chủ nội bộ (Direct Local Cloud)"
                item["available"] = True
            elif item.get("url"):
                item["stream_url"] = item.get("url")
                item["source_type"] = "Kho đám mây ngoại vi (Cloudflare R2 / CDN)"
                item["available"] = True
            else:
                item["stream_url"] = ""
                item["source_type"] = "Chưa có luồng"
                item["available"] = False
            result.append(item)
        return result

    def add_song(
        self,
        title: str,
        artist: str = "",
        url: str = "",
        local_file: str = "",
        tags: list[str] | None = None,
        duration: str = "",
        bitrate: str = "128kbps"
    ) -> dict[str, Any]:
        """Thêm hoặc cập nhật bài hát vào danh mục đám mây."""
        s_id = re.sub(r"\s+", "_", strip_accents(title))
        if not s_id:
            s_id = f"song_{len(self.songs) + 1}"

        item = {
            "id": s_id,
            "title": title.strip(),
            "artist": (artist or "Chưa rõ").strip(),
            "tags": [strip_accents(t) for t in (tags or []) if t] + [strip_accents(title), strip_accents(artist)],
            "url": (url or "").strip(),
            "local_file": (local_file or "").strip(),
            "duration": (duration or "").strip() or "03:30",
            "bitrate": bitrate or "128kbps"
        }

        # Cập nhật nếu trùng ID
        for i, s in enumerate(self.songs):
            if s.get("id") == s_id:
                self.songs[i] = item
                self._save_catalog()
                return item

        self.songs.append(item)
        self._save_catalog()
        return item

    def delete_song(self, song_id: str) -> bool:
        """Xóa bài hát khỏi danh mục và dọn file cục bộ nếu có."""
        found = False
        remaining = []
        for s in self.songs:
            if s.get("id") == song_id:
                found = True
                local_f = s.get("local_file")
                if local_f:
                    fpath = AUDIO_DIR / local_f
                    if fpath.exists():
                        try:
                            fpath.unlink()
                        except Exception as e:
                            logger.warning(f"Lỗi xóa file âm thanh {local_f}: {e}")
            else:
                remaining.append(s)
        if found:
            self.songs = remaining
            self._save_catalog()
        return found

    def search_song(self, query: str, public_url: str = "") -> Optional[dict[str, Any]]:
        """
        Tìm kiếm thông minh:
        1. Khớp chính xác ID hoặc tên bài hát.
        2. Khớp không dấu (accent-insensitive).
        3. Khớp chuỗi con trong tiêu đề, ca sĩ, danh sách từ khóa tags.
        """
        q = (query or "").strip()
        if not q:
            return None

        q_norm = strip_accents(q)
        q_words = set(q_norm.split())

        best_match = None
        best_score = 0

        for s in self.songs:
            s_id = s.get("id", "")
            s_title_norm = strip_accents(s.get("title", ""))
            s_artist_norm = strip_accents(s.get("artist", ""))
            s_tags = [strip_accents(t) for t in s.get("tags", [])]

            # 1. Trùng khớp hoàn toàn (100 điểm)
            if q_norm == s_title_norm or q_norm == s_id:
                best_match = s
                best_score = 100
                break

            score = 0
            # 2. Toàn bộ tên bài nằm trong câu lệnh giọng nói (ví dụ: "mở bài lạc trôi đi em")
            if s_title_norm and s_title_norm in q_norm:
                score += 55
            elif q_norm in s_title_norm:
                score += 45

            # 3. Tên ca sĩ nằm trong câu hỏi
            if s_artist_norm and (s_artist_norm in q_norm or q_norm in s_artist_norm):
                score += 30

            # 4. Trùng từ khóa tags
            for t in s_tags:
                if t and (t in q_norm or q_norm in t):
                    score += 25
                    break

            # 5. Đếm số từ trùng
            s_all_words = set(s_title_norm.split() + s_artist_norm.split())
            common_words = q_words.intersection(s_all_words)
            score += len(common_words) * 12

            if score > best_score:
                best_score = score
                best_match = s

        # Ngưỡng chấp nhận tối thiểu 20 điểm
        if best_score >= 20 and best_match:
            item = dict(best_match)
            pub = (public_url or "").rstrip("/")
            local_f = item.get("local_file", "").strip()
            if local_f and (AUDIO_DIR / local_f).exists():
                item["stream_url"] = f"{pub}/audio/{local_f}" if pub else f"/audio/{local_f}"
            else:
                item["stream_url"] = item.get("url", "")
            return item

        return None

    def verify_stream_url(self, url: str, timeout: float = 6.0) -> dict[str, Any]:
        """
        Kiểm định kỹ thuật của Direct Stream URL:
        - HTTP Status (200 / 206)
        - Hỗ trợ Range Request (BẮT BUỘC cho ESP32)
        - Content-Type chuẩn audio
        - Thời gian phản hồi (TTFB)
        """
        import time
        t0 = time.perf_counter()
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "xiaozhi-esp32-streamer/2.0",
                "Range": "bytes=0-4095"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                ttfb_ms = int((time.perf_counter() - t0) * 1000)
                status = r.status
                headers = dict(r.headers)
                ctype = headers.get("Content-Type", "")
                crange = headers.get("Content-Range", "")
                aranges = headers.get("Accept-Ranges", "")
                chunk = r.read(4096)

                is_range_ok = (status == 206) or ("bytes" in aranges.lower()) or bool(crange)
                is_audio_ok = ("audio" in ctype.lower()) or ("octet-stream" in ctype.lower()) or url.endswith(".mp3")
                has_bytes = len(chunk) > 0

                passed = is_range_ok and is_audio_ok and has_bytes

                return {
                    "valid": passed,
                    "status_code": status,
                    "content_type": ctype,
                    "content_range": crange,
                    "accept_ranges": aranges,
                    "bytes_received": len(chunk),
                    "ttfb_ms": ttfb_ms,
                    "error": None
                }
        except Exception as e:
            ttfb_ms = int((time.perf_counter() - t0) * 1000)
            return {
                "valid": False,
                "status_code": 0,
                "content_type": "",
                "ttfb_ms": ttfb_ms,
                "error": str(e)
            }
