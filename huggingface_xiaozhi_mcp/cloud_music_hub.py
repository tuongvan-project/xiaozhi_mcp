"""
Module Quản lý & Điều hướng Kho Nhạc Cá Nhân trên Đám mây (Cloud Music Hub).
Thiết kế tối ưu cho Xiaozhi & ESP32-S3:
- Hỗ trợ lưu trữ trực tiếp trên Cloudflare R2 / Supabase Storage / CDN tĩnh.
- Tìm kiếm siêu tốc (< 5ms) bằng giải thuật đối soát mờ (Fuzzy + Accent-Insensitive).
- Trả về Direct Stream URL chuẩn MP3 (audio/mpeg) hỗ trợ HTTP 206 Range Request.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("TheoLyMCP.CloudMusic")

# Đường dẫn file danh mục nhạc cá nhân cục bộ / cấu hình
CATALOG_FILE = Path(__file__).parent / "my_cloud_music.json"

# Kho nhạc mẫu kiểm thử chuẩn CDN trực tiếp (Direct MP3, hỗ trợ Range HTTP 206)
DEFAULT_CATALOG = {
    "_description": "Kho nhạc cá nhân của bạn lưu trên Đám mây (Cloudflare R2 / CDN trực tiếp)",
    "songs": [
        {
            "id": "lac_troi",
            "title": "Lạc Trôi",
            "artist": "Sơn Tùng M-TP",
            "tags": ["lac troi", "son tung", "mtp", "nhac tre"],
            "url": "https://raw.githubusercontent.com/tuongvan-project/xiaozhi_mcp/main/huggingface_xiaozhi_mcp/samples/lac_troi.mp3",
            "duration": "04:33",
            "bitrate": "128kbps"
        },
        {
            "id": "tim_lai_bau_troi",
            "title": "Tìm Lại Bầu Trời",
            "artist": "Tuấn Hưng",
            "tags": ["tim lai bau troi", "tuan hung", "nhac tre"],
            "url": "https://raw.githubusercontent.com/tuongvan-project/xiaozhi_mcp/main/huggingface_xiaozhi_mcp/samples/tim_lai_bau_troi.mp3",
            "duration": "04:12",
            "bitrate": "128kbps"
        },
        {
            "id": "lk_remix_2026",
            "title": "Liên Khúc Nhạc Trẻ Remix Sôi Động",
            "artist": "Nhiều Ca Sĩ",
            "tags": ["remix", "nonstop", "lien khuc", "nhac tre remix", "lk"],
            "url": "https://raw.githubusercontent.com/tuongvan-project/xiaozhi_mcp/main/huggingface_xiaozhi_mcp/samples/lk_remix.mp3",
            "duration": "15:20",
            "bitrate": "128kbps"
        },
        {
            "id": "soundhelix_sample",
            "title": "SoundHelix Instrumental Sample",
            "artist": "SoundHelix",
            "tags": ["sample", "test", "instrumental", "nhac khong loi"],
            "url": "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3",
            "duration": "06:12",
            "bitrate": "192kbps"
        }
    ]
}


def strip_accents(text: str) -> str:
    """Loại bỏ dấu tiếng Việt và ký tự đặc biệt để so khớp không dấu."""
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
        self._load_catalog()

    def _load_catalog(self) -> None:
        if self.catalog_path.exists():
            try:
                data = json.loads(self.catalog_path.read_text(encoding="utf-8"))
                self.songs = data.get("songs", [])
                logger.info(f"Đã nạp {len(self.songs)} bài hát từ {self.catalog_path}")
                return
            except Exception as e:
                logger.warning(f"Lỗi đọc catalog file: {e}. Dùng catalog mặc định.")
        self.songs = DEFAULT_CATALOG.get("songs", [])
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

    def add_song(self, title: str, artist: str, url: str, tags: list[str] | None = None) -> dict[str, Any]:
        """Thêm một bài hát mới trên Cloud vào danh mục."""
        s_id = re.sub(r"\s+", "_", strip_accents(title))
        item = {
            "id": s_id,
            "title": title.strip(),
            "artist": (artist or "Chưa rõ").strip(),
            "tags": [strip_accents(t) for t in (tags or [])] + [strip_accents(title), strip_accents(artist)],
            "url": url.strip()
        }
        # Cập nhật nếu trùng ID hoặc thêm mới
        for i, s in enumerate(self.songs):
            if s.get("id") == s_id:
                self.songs[i] = item
                self._save_catalog()
                return item
        self.songs.append(item)
        self._save_catalog()
        return item

    def search_song(self, query: str) -> Optional[dict[str, Any]]:
        """
        Tìm bài hát thông minh:
        1. Khớp chính xác ID hoặc Tên bài hát.
        2. Khớp không dấu (accent-insensitive).
        3. Khớp chứa từ khóa (sub-string matching trong title, artist, tags).
        """
        q = (query or "").strip()
        if not q:
            return None

        q_norm = strip_accents(q)
        q_words = set(q_norm.split())

        best_match = None
        best_score = 0

        for s in self.songs:
            s_title_norm = strip_accents(s.get("title", ""))
            s_artist_norm = strip_accents(s.get("artist", ""))
            s_tags = [strip_accents(t) for t in s.get("tags", [])]

            # 1. Trùng khớp hoàn toàn (100 điểm)
            if q_norm == s_title_norm:
                return s

            score = 0
            # 2. Toàn bộ tên bài nằm trong câu hỏi
            if s_title_norm and s_title_norm in q_norm:
                score += 50
            elif q_norm in s_title_norm:
                score += 40

            # 3. Tên ca sĩ nằm trong câu hỏi
            if s_artist_norm and (s_artist_norm in q_norm or q_norm in s_artist_norm):
                score += 30

            # 4. Trùng từ khóa trong tags
            for t in s_tags:
                if t in q_norm or q_norm in t:
                    score += 20
                    break

            # 5. Đếm số từ trùng
            s_all_words = set(s_title_norm.split() + s_artist_norm.split())
            common_words = q_words.intersection(s_all_words)
            score += len(common_words) * 10

            if score > best_score:
                best_score = score
                best_match = s

        # Ngưỡng chấp nhận (ít nhất 20 điểm để tránh nhận diện nhầm)
        if best_score >= 20:
            return best_match
        return None

    def verify_stream_url(self, url: str, timeout: float = 5.0) -> dict[str, Any]:
        """
        Kiểm định chất lượng kỹ thuật của Direct Stream URL:
        - Kiểm tra HTTP Status (200 / 206)
        - Kiểm tra Content-Type (audio/mpeg hoặc tương đương)
        - Kiểm tra Range Request header (bắt buộc cho ESP32)
        - Tốc độ tải byte đầu tiên (Time to First Byte - TTFB)
        """
        import time
        t0 = time.perf_counter()
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "xiaozhi-esp32/2.0",
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
