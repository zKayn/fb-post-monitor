"""
FB POST MONITOR (FAST SCAN + BATCH API + AUTO PART 2/3/4)
=================================================================
Theo dõi tất cả (hoặc 1 phần) Page Facebook bạn quản lý. Gửi thông báo
Telegram khi 1 bài đạt ngưỡng (OR nhiều điều kiện). Bỏ qua bài đã có link. Tự báo lỗi qua
Telegram (token hỏng, mất mạng...). Tự cảnh báo trước khi token hết hạn.
Khi 1 bài đạt ngưỡng và CHƯA có link, tự dùng OpenAI API viết tiếp
Part 2 + Part 3 + Part 4 dựa trên caption gốc, xuất ra file TXT UTF-8, gửi kèm
thông báo Telegram.

YÊU CẦU
--------
1. USER ACCESS TOKEN (Long-Lived) với đủ 4 quyền: pages_show_list,
   pages_read_engagement, pages_read_user_content, read_insights
2. Telegram Bot Token + Chat ID
3. OpenAI API Key (lấy tại platform.openai.com/api-keys, cần nạp tiền
   riêng, khác với ChatGPT Plus)
4. Cài thư viện: pip install requests
"""

import json
import os
import re
import time
import requests
import threading
import queue
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Domain link bài đọc. Có thể ghi nhiều domain trong Railway Variable ARTICLE_LINK_DOMAINS, cách nhau bằng dấu phẩy.
ARTICLE_LINK_DOMAINS = [
    d.strip().lower()
    for d in os.getenv("ARTICLE_LINK_DOMAINS", "puretales.cafex.biz").split(",")
    if d.strip()
]
COMMENT_LINK_MAX_PAGES = 5  # quét tối đa 5 trang x 100 comments trước khi báo / gọi OpenAI

# --- Tự viết Part 2 + Part 3 + Part 4 bằng 3 OpenAI API request liên tiếp ---
ENABLE_STORY_CONTINUATION = True
OPENAI_MODEL = "gpt-5.6-luna"
OPENAI_MAX_OUTPUT_TOKENS = 8000  # dư địa cho MỖI part; tránh reasoning/length làm content rỗng
OPENAI_TIMEOUT_SECONDS = 600
OPENAI_PART_RETRIES = 3  # chỉ retry lỗi API/network/content rỗng; không regenerate nội dung ngắn

STORY_WRITING_RULES = """You are a professional storyteller and novelist, skilled at creating emotionally rich, character-driven fiction that appeals to a broad mainstream audience.

Write ONLY the requested story part.

Requirements:
• Write in a natural, immersive novelistic style with smooth pacing and engaging storytelling.
• Focus on believable characters, emotional depth, family relationships, personal growth, trust, forgiveness, resilience, and meaningful life choices.
• Develop suspense through hidden truths, difficult decisions, misunderstandings, subtle clues, and gradual revelations rather than intense confrontations or sensational events.
• Maintain reader curiosity with realistic dialogue, layered character motivations, emotional conflict, and unexpected yet believable discoveries.
• Keep all events emotionally grounded and plausible.
• Avoid graphic violence, physical abuse, domestic abuse, cruelty, intimidation, revenge fantasies, public humiliation, excessive threats, manipulation, or abuse of power.
• Avoid disturbing, traumatic, or highly emotional scenes involving children or vulnerable characters. If children appear, portray them in safe, supportive, and age-appropriate situations.
• Avoid sensational plot twists designed only for shock value. Instead, build tension through mystery, relationships, and meaningful character choices.
• Use vivid descriptions, authentic dialogue, and emotionally resonant storytelling suitable for a wide audience.
• Allow supporting characters to have meaningful roles, realistic motivations, and emotional growth.
• Maintain a warm, family-friendly tone suitable for mainstream publishing platforms and advertising-friendly content standards.
• Length: approximately 2000-2500 words for this part. Aim to complete the full requested length in this single response.
• Maintain strict consistency in characters, settings, timeline, facts, relationships, and unresolved clues from all story context supplied above.
• Do NOT repeat scenes or recap large portions unnecessarily. Continue naturally from the exact point where the previous part ended.
• IMPORTANT: Do NOT write any END OF PART line, NEXT PART line, Facebook CTA, like/share request, or other ending marker. The program will append the correct ending line only after the part has been fully generated.
"""

PART_ENDINGS = {
    2: '-END OF PART 2, –PRESS NEX PART IN BOTTOM OF PAGE TO READ NEXT PART',
    3: '-END OF PART 3–PRESS NEX PART IN BOTTOM OF PAGE TO READ NEXT PART',
    4: '- END OF PART 4 ​​- LET SAY YES AND LIKE, SHARE THIS POST IN FACEBOOK SO THAT WE HAVE THE MOTIVATION TO SHARE MORE STORIES',
}

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
THRESHOLD_RULES = [
    {"min_views": 5000, "min_comments": 0},
    {"min_views": 3000, "min_comments": 40},
]
COMMENT_ONLY_THRESHOLD = 50  # comments vượt mốc này thì báo luôn, không cần xét views

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 120

# --- Phát hiện "dựng đứng" (viral spike) dựa trên tốc độ tăng views ---
SPIKE_LOOKBACK_MINUTES = 30
SPIKE_MIN_VIEW_INCREASE = 3000
SPIKE_MIN_PERCENT_INCREASE = 80
SPIKE_MIN_VIEWS_TO_CHECK = 2000
VIEW_HISTORY_FILE = "/data/view_history.json"
STORY_COMPLETED_FILE = "/data/story_completed_posts.json"  # chỉ đánh dấu sau khi TXT đủ Part 2+3+4 đã gửi thành công

# --- Retry khi gặp lỗi mạng tạm thời ---
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 3  # 3s, 6s, 9s...; cộng thêm retry ở HTTP adapter

# --- Batch API ---
# Mỗi bài viết cần 3 sub-request: 2 request insights riêng biệt (giữ cơ chế
# fallback metric để không mất dữ liệu nếu 1 metric không áp dụng được cho
# bài đó) + 1 request lấy message/comments/link. 16 bài x 3 = 48, dưới giới
# hạn tối đa 50 sub-request/batch của Facebook.
POSTS_PER_BATCH = 16
METRIC_FALLBACKS = ["post_media_view", "post_total_media_view_unique"]

# --- Cảnh báo token sắp hết hạn ---
TOKEN_EXPIRY_WARNING_DAYS = 5

CHECK_INTERVAL_SECONDS = 60
PAGE_DISCOVERY_INTERVAL_SECONDS = 900
FULL_POST_DISCOVERY_INTERVAL_SECONDS = 300
FAST_POST_AGE_HOURS = 12
FAST_NEAR_THRESHOLD_RATIO = 0.65
SLOW_POST_CHECK_INTERVAL_SECONDS = 300
PAGE_WORKERS = 3
TRACKED_POSTS_FILE = "/data/tracked_posts.json"
POSTS_PAGE_LIMIT = 100
POSTS_MAX_PAGES = 20

GRAPH_API_VERSION = "v20.0"
NOTIFIED_FILE = "/data/notified_posts.json"
# ==============================================================

GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)


# ==================== RETRY HELPER ====================
def _build_http_session():
    # Retry cả lỗi connect/read và HTTP tạm thời. POST được phép retry vì các POST
    # Facebook ở đây chỉ là Batch API đọc dữ liệu; Telegram/OpenAI vẫn có retry riêng.
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET", "POST")),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

HTTP_SESSION = _build_http_session()

def api_get(url, params=None, timeout=20):
    """GET có nhiều lớp retry; chỉ raise sau khi đã thử hết."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return HTTP_SESSION.get(url, params=params, timeout=(10, timeout))
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[CẢNH BÁO] GET lỗi mạng (vòng {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(min(RETRY_BACKOFF_SECONDS * attempt, 20))
    raise last_exc

def api_post(url, data=None, timeout=30):
    """POST có nhiều lớp retry; dùng cho Facebook Batch/Telegram text."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return HTTP_SESSION.post(url, data=data, timeout=(10, timeout))
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[CẢNH BÁO] POST lỗi mạng (vòng {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(min(RETRY_BACKOFF_SECONDS * attempt, 20))
    raise last_exc


def meets_threshold(views: int, comments: int) -> bool:
    if comments > COMMENT_ONLY_THRESHOLD:
        return True
    for rule in THRESHOLD_RULES:
        if views >= rule["min_views"] and comments >= rule["min_comments"]:
            return True
    return False


# -------------------- Lưu/đọc trạng thái đã báo --------------------
def load_notified():
    if os.path.exists(NOTIFIED_FILE):
        try:
            with open(NOTIFIED_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_notified(notified_set):
    try:
        os.makedirs(os.path.dirname(NOTIFIED_FILE) or ".", exist_ok=True)
        with open(NOTIFIED_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(notified_set), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LỖI] Không lưu được {NOTIFIED_FILE}: {e}")


already_notified = load_notified()

def load_story_completed():
    if os.path.exists(STORY_COMPLETED_FILE):
        try:
            with open(STORY_COMPLETED_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_story_completed(items):
    try:
        os.makedirs(os.path.dirname(STORY_COMPLETED_FILE) or ".", exist_ok=True)
        with open(STORY_COMPLETED_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(items), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LỖI] Không lưu được {STORY_COMPLETED_FILE}: {e}")

story_completed = load_story_completed()
# already_spike_notified không dùng set riêng nữa -> lưu chung vào already_notified
# với tiền tố "spike_" để không bị mất khi restart (trước đây chỉ lưu trong bộ nhớ).


# -------------------- Lưu/đọc lịch sử views (spike detection) --------------------
def load_view_history():
    if os.path.exists(VIEW_HISTORY_FILE):
        try:
            with open(VIEW_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_view_history(history: dict):
    try:
        os.makedirs(os.path.dirname(VIEW_HISTORY_FILE) or ".", exist_ok=True)
        with open(VIEW_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LỖI] Không lưu được {VIEW_HISTORY_FILE}: {e}")


view_history = load_view_history()


def record_and_check_spike(post_id: str, current_views: int, now: datetime):
    points = view_history.get(post_id, [])
    cutoff = now - timedelta(minutes=SPIKE_LOOKBACK_MINUTES)
    baseline_views = None
    for ts_str, v in points:
        ts = datetime.fromisoformat(ts_str)
        if ts <= cutoff:
            baseline_views = v
        else:
            break

    points.append([now.isoformat(), current_views])
    view_history[post_id] = points[-200:]

    if current_views < SPIKE_MIN_VIEWS_TO_CHECK:
        return False
    if baseline_views is None:
        return False

    increase = current_views - baseline_views
    percent_increase = (increase / baseline_views * 100) if baseline_views > 0 else 0
    return increase >= SPIKE_MIN_VIEW_INCREASE or percent_increase >= SPIKE_MIN_PERCENT_INCREASE


# -------------------- Telegram --------------------
TELEGRAM_GROUP_LOCK = threading.RLock()  # Không cho tin khác chen giữa thông báo và 3 TXT.

def _send_telegram_message_unlocked(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = api_post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        if resp.status_code != 200:
            print(f"[LỖI] Gửi Telegram thất bại: {resp.text}")
    except Exception as e:
        print(f"[LỖI] Gửi Telegram thất bại (mạng): {e}")


def send_telegram_message(text: str):
    with TELEGRAM_GROUP_LOCK:
        return _send_telegram_message_unlocked(text)


def _send_telegram_document_unlocked(file_path: str, caption: str = ""):
    """Gửi file Telegram với retry để giảm lỗi mạng tạm thời."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(file_path, "rb") as f:
                resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024]}, files={"document": f}, timeout=180)
            if resp.status_code == 200:
                return True
            last_error = f"HTTP {resp.status_code}: {resp.text[:1000]}"
        except Exception as e:
            last_error = str(e)
        print(f"[CẢNH BÁO] Gửi file Telegram lần {attempt}/{MAX_RETRIES} thất bại: {last_error}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    send_error_alert("telegram_document_failed", f"Gửi file TXT thất bại sau {MAX_RETRIES} lần: {last_error}")
    return False

def send_telegram_document(file_path: str, caption: str = ""):
    with TELEGRAM_GROUP_LOCK:
        return _send_telegram_document_unlocked(file_path, caption)




# -------------------- Tự viết Part 2 + Part 3 + Part 4 (3 request liên tiếp) --------------------
def call_openai_part(story_context: str, part_number: int):
    """Sinh đúng 1 part. Chỉ trả content; ending được nối đúng 1 lần sau khi part thành công."""
    if not OPENAI_API_KEY:
        print("[CẢNH BÁO] Chưa cấu hình OPENAI_API_KEY, bỏ qua bước viết Part 2/3/4.")
        return None

    if part_number == 2:
        part_instruction = """Write PART 2 only.
Create ONE engaging, curiosity-driven headline before PART 2.
End the STORY CONTENT with a believable revelation or discovery leading naturally into Part 3.
Do not write Part 3 or Part 4."""
    elif part_number == 3:
        part_instruction = """Write PART 3 only.
Do not create a new headline. Continue directly from Part 2.
End the STORY CONTENT with a believable revelation or discovery leading naturally into Part 4.
Do not rewrite Part 2 and do not write Part 4."""
    elif part_number == 4:
        part_instruction = """Write PART 4 (The End) only.
Do not create a new headline. Continue directly from Part 3.
Resolve the important remaining threads with a satisfying, emotionally relieving ending.
Do not rewrite earlier parts."""
    else:
        raise ValueError(f"part_number không hợp lệ: {part_number}")

    full_prompt = f"""STORY CONTEXT (continuity reference):

{story_context}

--- WRITING RULES ---
{STORY_WRITING_RULES}

--- CURRENT TASK ---
{part_instruction}

Start now. Do NOT output END OF PART / NEXT PART / Facebook CTA lines."""

    last_error = "unknown"
    for attempt in range(1, OPENAI_PART_RETRIES + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": full_prompt}],
                    "max_completion_tokens": OPENAI_MAX_OUTPUT_TOKENS,
                },
                timeout=OPENAI_TIMEOUT_SECONDS,
            )
            try:
                data = resp.json()
            except Exception:
                data = {}

            if resp.status_code != 200:
                last_error = data.get("error", {}).get("message", resp.text[:1000])
                print(f"[CẢNH BÁO] Part {part_number} HTTP {resp.status_code}, lần {attempt}/{OPENAI_PART_RETRIES}: {last_error}")
            else:
                choices = data.get("choices") or []
                if choices:
                    choice = choices[0] or {}
                    msg = choice.get("message") or {}
                    text = msg.get("content") or ""
                    if isinstance(text, list):
                        text = "".join(x.get("text", "") if isinstance(x, dict) else str(x) for x in text)
                    text = str(text).strip()
                    finish_reason = choice.get("finish_reason")
                    usage = data.get("usage", {})
                    if text:
                        print(f"[OPENAI] Part {part_number} OK | finish_reason={finish_reason} | usage={usage}")
                        return text
                    last_error = f"content rỗng; finish_reason={finish_reason}; usage={usage}"
                else:
                    last_error = f"không có choices; response={str(data)[:1200]}"
                print(f"[CẢNH BÁO] Part {part_number} rỗng/lỗi, lần {attempt}/{OPENAI_PART_RETRIES}: {last_error}")
        except requests.exceptions.RequestException as e:
            last_error = f"network/timeout: {e}"
            print(f"[CẢNH BÁO] Part {part_number}, lần {attempt}/{OPENAI_PART_RETRIES}: {last_error}")
        except Exception as e:
            last_error = f"parse/process: {e}"
            print(f"[CẢNH BÁO] Part {part_number}, lần {attempt}/{OPENAI_PART_RETRIES}: {last_error}")

        if attempt < OPENAI_PART_RETRIES:
            time.sleep(min(RETRY_BACKOFF_SECONDS * attempt, 20))

    send_error_alert(f"openai_api_part_{part_number}_failed", f"Không tạo được Part {part_number} sau {OPENAI_PART_RETRIES} lần thử. Chi tiết cuối: {last_error}")
    return None


def _strip_program_endings(text: str) -> str:
    """Loại ending/CTA nếu model lỡ tự sinh; code sẽ chèn đúng 1 lần sau validation."""
    if not text:
        return ""
    cleaned = text.strip()
    for ending in PART_ENDINGS.values():
        for variant in (ending, f"**{ending}**"):
            cleaned = cleaned.replace(variant, "")
    # Loại các dòng CTA phổ biến mà model có thể tự thêm ngoài ý muốn.
    cleaned = re.sub(r"(?im)^\s*[-–—]*\s*END OF PART [234].*$", "", cleaned)
    cleaned = re.sub(r"(?im)^.*PRESS NEX(?:T)? PART.*$", "", cleaned)
    cleaned = re.sub(r"(?im)^.*LIKE,? SHARE THIS POST.*$", "", cleaned)
    return cleaned.strip()


def _ensure_part_header(text: str, part_number: int) -> str:
    """Đảm bảo mỗi phần có nhãn PART rõ ràng mà không làm mất headline của Part 2."""
    text = text.strip()
    pattern = rf"(?im)^\s*\*{{0,2}}PART\s+{part_number}(?:\s*\(THE END\))?\*{{0,2}}\s*$"
    if re.search(pattern, text):
        return text
    if part_number == 2:
        lines = text.splitlines()
        # Giữ dòng đầu làm headline, chèn PART 2 ngay sau headline.
        if len(lines) >= 2:
            return lines[0].strip() + "\n\nPART 2\n\n" + "\n".join(lines[1:]).strip()
    label = "PART 4 (THE END)" if part_number == 4 else f"PART {part_number}"
    return f"{label}\n\n{text}"


def _validate_part(text: str, part_number: int):
    """Validation trước khi cho phép đi tiếp. Không đạt => coi như request lỗi và không tạo TXT."""
    if not text or not text.strip():
        return False, "content rỗng"
    words = len(re.findall(r"\b[\w’'-]+\b", text, flags=re.UNICODE))
    # Yêu cầu prompt là 2000-2500 từ. Cho biên an toàn để tránh loại nhầm văn bản tốt.
    if words < 1800:
        return False, f"quá ngắn ({words} từ; tối thiểu an toàn 1800 sau bước bổ sung)"
    if part_number in (3, 4) and not re.search(rf"(?i)PART\s+{part_number}", text):
        return False, f"thiếu nhãn PART {part_number}"
    return True, f"OK ({words} từ)"


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w’'-]+\b", text or "", flags=re.UNICODE))


def call_openai_continue_part(story_context: str, existing_part: str, part_number: int, target_words: int = 2200):
    """Bổ sung phần còn thiếu thay vì vứt content đã trả tiền và generate lại từ đầu."""
    current_words = _word_count(existing_part)
    need_words = max(250, target_words - current_words)
    # Cho dư nhẹ để model có thể kết thúc tự nhiên, nhưng tránh sinh quá dài/tốn tiền.
    requested_words = min(max(need_words + 150, 350), 1200)

    if part_number in (2, 3):
        ending_instruction = f"End with a natural hook or discovery leading into Part {part_number + 1}."
    else:
        ending_instruction = "Resolve the remaining story threads with a satisfying, emotionally relieving ending."

    prompt = f"""You are continuing an existing story part. DO NOT rewrite, summarize, or repeat text already written.

STORY CONTEXT:
{story_context}

EXISTING PART {part_number} (already paid for and must be preserved):
{existing_part}

TASK:
Continue PART {part_number} from the exact final sentence above. Write approximately {requested_words} additional words so the COMPLETE part reaches about 2000-2500 words.
Maintain exact continuity, characters, timeline, tone, and facts.
{ending_instruction}
Do NOT add a PART heading, headline, END OF PART line, NEXT PART line, Facebook CTA, like/share request, or commentary.
Output ONLY the new continuation paragraphs."""

    last_error = "unknown"
    for attempt in range(1, OPENAI_PART_RETRIES + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    # Continuation is intentionally much smaller than a full-part generation.
                    "max_completion_tokens": min(OPENAI_MAX_OUTPUT_TOKENS, 3500),
                },
                timeout=OPENAI_TIMEOUT_SECONDS,
            )
            try:
                data = resp.json()
            except Exception:
                data = {}

            if resp.status_code != 200:
                last_error = data.get("error", {}).get("message", resp.text[:1000])
            else:
                choices = data.get("choices") or []
                if choices:
                    choice = choices[0] or {}
                    msg = choice.get("message") or {}
                    extra = msg.get("content") or ""
                    if isinstance(extra, list):
                        extra = "".join(x.get("text", "") if isinstance(x, dict) else str(x) for x in extra)
                    extra = _strip_program_endings(str(extra).strip())
                    if extra:
                        usage = data.get("usage", {})
                        print(f"[OPENAI] Part {part_number} continuation OK | +{_word_count(extra)} từ | usage={usage}")
                        return extra
                    last_error = f"continuation rỗng; finish_reason={choice.get('finish_reason')}; usage={data.get('usage', {})}"
                else:
                    last_error = f"không có choices; response={str(data)[:1200]}"
        except requests.exceptions.RequestException as e:
            last_error = f"network/timeout: {e}"
        except Exception as e:
            last_error = f"parse/process: {e}"

        print(f"[CẢNH BÁO] Bổ sung Part {part_number} lần {attempt}/{OPENAI_PART_RETRIES} thất bại: {last_error}")
        if attempt < OPENAI_PART_RETRIES:
            time.sleep(min(RETRY_BACKOFF_SECONDS * attempt, 20))

    return None


def generate_valid_part(story_context: str, part_number: int):
    """Sinh 1 lần; nếu ngắn thì GIỮ content và chỉ mua thêm phần còn thiếu."""
    raw = call_openai_part(story_context, part_number)
    if not raw:
        send_error_alert(
            f"openai_part_{part_number}_generation_failed",
            f"Part {part_number} không tạo được content nên KHÔNG xuất TXT.",
        )
        return None

    raw = _strip_program_endings(raw)
    raw = _ensure_part_header(raw, part_number)
    words = _word_count(raw)

    # Nếu >= 1400 nhưng dưới mục tiêu, tuyệt đối không regenerate toàn bộ.
    # Giữ content và gọi continuation ngắn để đạt ~2000-2500 từ.
    continuation_rounds = 3
    while words < 1900 and continuation_rounds > 0:
        print(f"[OPENAI] Part {part_number} mới có {words} từ -> giữ nguyên và viết bổ sung, KHÔNG regenerate.")
        extra = call_openai_continue_part(story_context, raw, part_number, target_words=2200)
        if not extra:
            break
        raw = f"{raw.rstrip()}\n\n{extra.strip()}"
        raw = _strip_program_endings(raw)
        words = _word_count(raw)
        continuation_rounds -= 1

    ok, reason = _validate_part(raw, part_number)
    if ok:
        print(f"[OPENAI] Part {part_number} validation: {reason}")
        return raw

    send_error_alert(
        f"openai_part_{part_number}_validation_failed",
        f"Part {part_number} chưa đạt điều kiện hoàn chỉnh nên KHÔNG xuất TXT. Lý do cuối: {reason}",
    )
    return None


def call_openai_story(caption: str):
    """ALL-OR-NOTHING: chỉ trả story khi Part 2, Part 3 và Part 4 đều hoàn chỉnh."""
    print("[OPENAI] Request 1/3: Đang viết Part 2...")
    p2_raw = generate_valid_part(caption, 2)
    if not p2_raw:
        return None

    print("[OPENAI] Request 2/3: Đang viết Part 3 với context Part 1 + Part 2...")
    p3_raw = generate_valid_part(f"{caption}\n\n{p2_raw}", 3)
    if not p3_raw:
        return None

    print("[OPENAI] Request 3/3: Đang viết Part 4 với context Part 1 + Part 2 + Part 3...")
    p4_raw = generate_valid_part(f"{caption}\n\n{p2_raw}\n\n{p3_raw}", 4)
    if not p4_raw:
        return None

    # Ending chỉ chèn sau khi cả content của part đó đã hoàn chỉnh.
    # Chỉ kẻ 1 đường phân cách PHÍA TRÊN Part 3 và Part 4 để dễ copy từ TXT.
    # Không có đường kẻ phía dưới tiêu đề Part.
    separator = "=" * 60
    part2 = f"{p2_raw}\n\n**{PART_ENDINGS[2]}**"
    part3 = f"{separator}\n\n{p3_raw}\n\n**{PART_ENDINGS[3]}**"
    part4 = f"{separator}\n\n{p4_raw}\n\n**{PART_ENDINGS[4]}**"
    return "\n\n".join((part2, part3, part4))


def validate_complete_story(story_text: str):
    """Cổng cuối: TXT tuyệt đối không được tạo nếu thiếu bất kỳ Part 2/3/4 hoặc ending."""
    if not story_text:
        return False, "story_text rỗng"
    for n in (2, 3, 4):
        if not re.search(rf"(?i)PART\s+{n}", story_text):
            return False, f"thiếu PART {n}"
        if story_text.count(PART_ENDINGS[n]) != 1:
            return False, f"ending Part {n} không xuất hiện đúng 1 lần"
    return True, "đủ Part 2 + Part 3 + Part 4"


def split_story_into_parts(story_text: str):
    """Tách story đã hoàn chỉnh thành 3 nội dung TXT độc lập, không gọi lại OpenAI."""
    ok, reason = validate_complete_story(story_text)
    if not ok:
        raise ValueError(f"Từ chối tách TXT: {reason}")

    separator = "=" * 60
    chunks = story_text.split(separator)
    if len(chunks) != 3:
        raise ValueError(f"Không thể tách chính xác 3 Part (tìm thấy {len(chunks)} khối)")

    parts = {}
    for n, chunk in zip((2, 3, 4), chunks):
        clean = chunk.replace("**", "").strip() + "\n"
        if not re.search(rf"(?i)PART\s+{n}", clean):
            raise ValueError(f"Khối TXT thứ {n - 1} không chứa PART {n}")
        if clean.count(PART_ENDINGS[n]) != 1:
            raise ValueError(f"TXT Part {n} không có ending đúng 1 lần")
        parts[n] = clean
    return parts


def create_part_txt(part_text: str, part_number: int, out_path: str):
    """Ghi đúng một Part thành một file TXT UTF-8 riêng."""
    if not re.search(rf"(?i)PART\s+{part_number}", part_text):
        raise ValueError(f"Từ chối tạo TXT: thiếu PART {part_number}")
    if part_text.count(PART_ENDINGS[part_number]) != 1:
        raise ValueError(f"Từ chối tạo TXT: ending Part {part_number} không đúng 1 lần")

    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(part_text.strip() + "\n")

    if not os.path.exists(out_path) or os.path.getsize(out_path) < 500:
        raise IOError(f"File Part {part_number} TXT không tồn tại hoặc kích thước bất thường")
    return True


def generate_and_send_story_continuation(page_name: str, post_id: str, caption: str):
    """Tạo Part 2/3/4 một lần bằng OpenAI, sau đó xuất và gửi 3 file TXT riêng."""
    global story_completed
    if not ENABLE_STORY_CONTINUATION:
        return False
    if not caption or not caption.strip():
        print(f"[CẢNH BÁO] Bài {post_id} không có caption, bỏ qua viết Part 2/3/4.")
        return False

    story_key = str(post_id)
    if story_key in story_completed:
        return True

    print(f"[OPENAI] Bắt đầu ALL-OR-NOTHING Part 2/3/4 cho bài {post_id}...")
    story_text = call_openai_story(caption)
    ok, reason = validate_complete_story(story_text)
    if not ok:
        print(f"[OPENAI] KHÔNG tạo TXT cho {post_id}: {reason}")
        send_error_alert(
            f"story_incomplete_{post_id}",
            f"Bài {post_id}: chưa đủ Part 2/3/4 ({reason}). Không xuất TXT; hệ thống sẽ thử lại ở lượt quét sau.",
        )
        return False

    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", post_id)
    txt_paths = {
        2: f"/tmp/story_{safe_id}_PART_2.txt",
        3: f"/tmp/story_{safe_id}_PART_3.txt",
        4: f"/tmp/story_{safe_id}_PART_4.txt",
    }

    try:
        # Chỉ tách kết quả đã tạo; KHÔNG phát sinh request OpenAI mới.
        parts = split_story_into_parts(story_text)
        for n in (2, 3, 4):
            create_part_txt(parts[n], n, txt_paths[n])

        # Gửi tuần tự để Telegram hiển thị Part 2 -> Part 3 -> Part 4 ngay sau thông báo bài đang lên.
        for n in (2, 3, 4):
            sent = send_telegram_document(
                txt_paths[n],
                caption=f"📄 PART {n} — Page: {page_name}",
            )
            if not sent:
                raise RuntimeError(f"Gửi TXT Part {n} thất bại")

        # Chỉ đánh dấu hoàn thành sau khi cả 3 file đều gửi thành công.
        story_completed.add(story_key)
        save_story_completed(story_completed)
        print(f"[OPENAI] Đã gửi 3 TXT riêng Part 2, Part 3, Part 4 cho bài {post_id}.")
        return True
    except Exception as e:
        print(f"[LỖI] Không tạo/gửi đủ 3 file TXT: {e}")
        send_error_alert(
            f"story_txt_error_{post_id}",
            f"Bài {post_id}: không tạo/gửi đủ 3 TXT: {e}. Hệ thống sẽ thử lại.",
        )
        return False
    finally:
        for path in txt_paths.values():
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


# Persist one-time notifications across Railway restarts (mount /data as a volume).
ONE_TIME_EVENTS_FILE = "/data/one_time_events.json"
def _load_one_time_events():
    try:
        with open(ONE_TIME_EVENTS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (OSError, ValueError, TypeError):
        return set()

_one_time_events = _load_one_time_events()
_one_time_lock = threading.RLock()

def notify_once(event_key, text):
    """One Telegram message per event key, including after process restart."""
    with _one_time_lock:
        if event_key in _one_time_events:
            return False
        # Reserve before sending, so concurrent workers cannot send duplicates.
        _one_time_events.add(event_key)
        try:
            os.makedirs(os.path.dirname(ONE_TIME_EVENTS_FILE), exist_ok=True)
            with open(ONE_TIME_EVENTS_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(_one_time_events), f, ensure_ascii=False)
        except OSError as e:
            print(f"[WARN] Cannot persist one-time event {event_key}: {e}")
        send_telegram_message(text)
        return True

def send_error_alert(error_key: str, text: str):
    return notify_once(f"error:{error_key}", f"⚠️ LỖI SCRIPT!\n{text}\n\nThời gian: {datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")

# -------------------- Cảnh báo token sắp hết hạn --------------------
def check_token_expiry():
    """Tự hỏi Facebook token còn bao lâu hết hạn, cảnh báo nếu sắp hết."""
    try:
        r = api_get(
            f"{GRAPH_URL}/debug_token",
            params={"input_token": USER_ACCESS_TOKEN, "access_token": USER_ACCESS_TOKEN},
        )
        data = r.json().get("data", {})
    except Exception as e:
        print(f"[CẢNH BÁO] Không kiểm tra được hạn token: {e}")
        return

    if not data.get("is_valid", True):
        send_error_alert(
            "token_invalid",
            "USER_ACCESS_TOKEN không còn hợp lệ (có thể đã hết hạn hoặc bị thu hồi)! "
            "Hãy lấy token mới và cập nhật vào Railway Variables ngay.",
        )
        return

    expires_at = data.get("expires_at")
    if not expires_at:  # 0 hoặc None = token không có hạn / không xác định được
        return

    expire_dt = datetime.fromtimestamp(expires_at)
    days_left = (expire_dt - datetime.now()).days
    if days_left <= TOKEN_EXPIRY_WARNING_DAYS:
        send_error_alert(
            "token_expiry_warning",
            f"USER_ACCESS_TOKEN sắp hết hạn! Còn khoảng {days_left} ngày "
            f"(hết hạn lúc {expire_dt.strftime('%H:%M %d/%m/%Y')}).\n"
            "Hãy lấy Long-Lived Token mới tại Graph API Explorer và cập nhật "
            "vào Railway -> Variables -> USER_ACCESS_TOKEN.",
        )


# -------------------- Chống spam cảnh báo lỗi mạng Facebook --------------------
FB_NETWORK_ALERT_AFTER_FAILURES = 3
_fb_network_failures = {}
_fb_network_alerted = set()

def note_fb_network_failure(error_key: str, text: str):
    """Chỉ Telegram sau 3 vòng lỗi liên tiếp; lỗi thoáng qua chỉ log."""
    count = _fb_network_failures.get(error_key, 0) + 1
    _fb_network_failures[error_key] = count
    print(f"[FACEBOOK NETWORK] {error_key}: lỗi liên tiếp {count}/{FB_NETWORK_ALERT_AFTER_FAILURES} - {text}")
    if count >= FB_NETWORK_ALERT_AFTER_FAILURES and error_key not in _fb_network_alerted:
        send_error_alert(error_key, f"Facebook mất kết nối {count} lần liên tiếp.\n{text}")
        _fb_network_alerted.add(error_key)

def note_fb_network_success(error_key: str):
    """Reset bộ đếm khi request Facebook thành công."""
    had_failures = _fb_network_failures.get(error_key, 0)
    was_alerted = error_key in _fb_network_alerted
    _fb_network_failures[error_key] = 0
    _fb_network_alerted.discard(error_key)
    if was_alerted:
        notify_once(f"recovered:{error_key}", "✅ FACEBOOK API ĐÃ KẾT NỐI LẠI. Hệ thống tiếp tục quét bình thường.")
    elif had_failures:
        print(f"[FACEBOOK NETWORK] {error_key}: kết nối đã phục hồi sau {had_failures} lần lỗi.")

# -------------------- Facebook API --------------------
def get_managed_pages():
    pages = []
    url = f"{GRAPH_URL}/me/accounts"
    params = {"fields": "id,name,access_token", "limit": 100, "access_token": USER_ACCESS_TOKEN}
    while url:
        try:
            r = api_get(url, params=params)
        except Exception as e:
            note_fb_network_failure("get_managed_pages_network", f"Không kết nối được Facebook để lấy danh sách Page: {e}")
            return []
        note_fb_network_success("get_managed_pages_network")
        data = r.json()
        if "error" in data:
            err_msg = data["error"].get("message", "Không rõ nguyên nhân")
            print(f"[LỖI] Không lấy được danh sách Page: {err_msg}")
            send_error_alert(
                "get_managed_pages",
                f"Không lấy được danh sách Page (có thể USER_ACCESS_TOKEN đã hết hạn/sai quyền).\nChi tiết: {err_msg}",
            )
            return []
        pages.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")
        url = next_url
        params = None
    if INCLUDE_PAGE_NAMES:
        pages = [p for p in pages if p.get("name") in INCLUDE_PAGE_NAMES]
    return pages


def get_recent_post_ids(page_id: str, page_token: str):
    """Đọc nhiều trang /posts thay vì chỉ 25 bài đầu; không đánh dấu thiếu dữ liệu là đã xử lý."""
    url = f"{GRAPH_URL}/{page_id}/posts"
    params = {"fields": "id,created_time", "limit": POSTS_PAGE_LIMIT, "access_token": page_token}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ONLY_POSTS_NEWER_THAN_HOURS)
    found, seen = [], set()
    for page_no in range(POSTS_MAX_PAGES):
        try:
            r = api_get(url, params=params)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise ValueError(data["error"].get("message", "Facebook API error"))
        except Exception as e:
            note_fb_network_failure(f"posts_{page_id}", f"Page {page_id}: không lấy được trang {page_no+1}: {e}")
            return found  # các trang trước vẫn được kiểm tra; trang thiếu sẽ thử lại vòng sau
        note_fb_network_success(f"posts_{page_id}")
        oldest = None
        for post in data.get("data", []):
            pid, created = post.get("id"), post.get("created_time")
            if not pid or not created or pid in seen:
                continue
            try:
                dt = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S%z")
            except (ValueError, TypeError):
                continue
            oldest = min(oldest, dt) if oldest else dt
            if dt >= cutoff:
                found.append((pid, dt.isoformat()))
                seen.add(pid)
        url = data.get("paging", {}).get("next")
        params = None
        if not url or (oldest is not None and oldest < cutoff):
            break
    else:
        print(f"[CẢNH BÁO] Page {page_id}: đã quét {POSTS_MAX_PAGES} trang, có thể còn bài cũ hơn.")
    print(f"[FACEBOOK] Page {page_id}: tìm thấy {len(found)} bài trong {ONLY_POSTS_NEWER_THAN_HOURS} giờ.")
    return found


# Persist discovered posts so a fast scan does not have to paginate 120 hours each minute.
def load_tracked_posts():
    try:
        with open(TRACKED_POSTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def save_tracked_posts():
    try:
        os.makedirs(os.path.dirname(TRACKED_POSTS_FILE), exist_ok=True)
        tmp = TRACKED_POSTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(tracked_posts, f, ensure_ascii=False)
        os.replace(tmp, TRACKED_POSTS_FILE)
    except OSError as e:
        print(f"[CACHE] Không lưu được bài theo dõi: {e}")


tracked_posts = load_tracked_posts()
for _page_posts in tracked_posts.values():
    if isinstance(_page_posts, dict):
        for _entry in _page_posts.values():
            if isinstance(_entry, dict):
                _entry["last_check"] = 0
_last_discovery = {}
_last_page_discovery = 0.0
_cached_pages = []


def near_threshold(views, comments):
    if comments >= COMMENT_ONLY_THRESHOLD * FAST_NEAR_THRESHOLD_RATIO:
        return True
    return any(views >= rule["min_views"] * FAST_NEAR_THRESHOLD_RATIO and
               comments >= rule["min_comments"] * FAST_NEAR_THRESHOLD_RATIO
               for rule in THRESHOLD_RULES)


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def get_stats_batch(post_ids: list, page_token: str) -> dict:
    """Lấy views/comments/link/message của nhiều bài viết cùng lúc qua
    Facebook Batch API (tối đa POSTS_PER_BATCH bài/lần gọi HTTP).
    Mỗi bài dùng 2 sub-request insights riêng biệt theo METRIC_FALLBACKS
    (không gộp chung 1 câu truy vấn) để giữ cơ chế fallback: nếu metric A
    không áp dụng được cho bài đó, vẫn còn kết quả từ metric B, tránh mất
    dữ liệu oan uổng do 1 metric lỗi kéo cả bài xuống."""
    results = {}

    for group in chunked(post_ids, POSTS_PER_BATCH):
        batch_items = []
        for pid in group:
            for metric in METRIC_FALLBACKS:
                batch_items.append(
                    {
                        "method": "GET",
                        "relative_url": f"{pid}/insights?metric={metric}&period=lifetime",
                    }
                )
            batch_items.append(
                {
                    "method": "GET",
                    "relative_url": f"{pid}?fields=message,comments.summary(true),permalink_url",
                }
            )

        try:
            resp = api_post(
                f"{GRAPH_URL}/",
                data={"access_token": page_token, "batch": json.dumps(batch_items)},
            )
        except Exception as e:
            note_fb_network_failure("batch_request_network", f"Lỗi mạng khi gọi Batch API: {e}")
            continue

        note_fb_network_success("batch_request_network")
        try:
            batch_resp = resp.json()
        except Exception as e:
            print(f"[LỖI] Batch request lỗi parse JSON: {e}")
            continue

        if not isinstance(batch_resp, list):
            err = batch_resp.get("error", {}).get("message", str(batch_resp)) if isinstance(batch_resp, dict) else str(batch_resp)
            print(f"[LỖI] Batch request thất bại: {err}")
            send_error_alert("batch_request_error", f"Batch request thất bại: {err}")
            continue

        n_metrics = len(METRIC_FALLBACKS)
        items_per_post = n_metrics + 1  # 2 insights + 1 fields

        for idx, pid in enumerate(group):
            base = idx * items_per_post
            insight_items = batch_resp[base : base + n_metrics] if base + n_metrics <= len(batch_resp) else []
            fields_item = batch_resp[base + n_metrics] if base + n_metrics < len(batch_resp) else None

            # Thử lần lượt từng metric, lấy kết quả đầu tiên hợp lệ
            views = None
            for insight_item in insight_items:
                if insight_item and insight_item.get("code") == 200:
                    try:
                        body = json.loads(insight_item["body"])
                        data_list = body.get("data", [])
                        if data_list:
                            vals = data_list[0].get("values", [])
                            if vals and vals[-1].get("value") is not None:
                                views = vals[-1]["value"]
                                break
                    except Exception:
                        continue

            comments = None
            link = None
            message = ""
            if fields_item and fields_item.get("code") == 200:
                try:
                    body = json.loads(fields_item["body"])
                    message = body.get("message", "") or ""
                    if "comments" in body:
                        comments = body["comments"]["summary"]["total_count"]
                        link = body.get("permalink_url")
                except Exception:
                    pass
            elif fields_item and fields_item.get("code") != 200:
                try:
                    err_body = json.loads(fields_item["body"])
                    err_msg = err_body.get("error", {}).get("message", "Không rõ")
                except Exception:
                    err_msg = "Không rõ"
                print(f"[LỖI] {pid}: {err_msg}")

            results[pid] = (views, comments, link, message)

    return results


def contains_article_link(text: str) -> bool:
    """Nhận diện link bài đọc cần tránh tạo TXT. Domain cấu hình riêng để không nhầm URL Facebook/permalink."""
    value = (text or "").lower()
    return any(domain in value for domain in ARTICLE_LINK_DOMAINS)


def post_already_has_link(post_id: str, page_id: str, page_token: str, post_message: str):
    """Return True=link found, False=verified no link, None=unable to verify.

    Fail closed: Graph API permission/network errors must NEVER be treated as no link.
    """
    if URL_PATTERN.search(post_message or ""):
        return True
    url = f"{GRAPH_URL}/{post_id}/comments"
    params = {"filter": "stream", "limit": 100, "fields": "message,from", "access_token": page_token}
    pages_checked = 0
    while url and pages_checked < COMMENT_LINK_MAX_PAGES:
        try:
            r = api_get(url, params=params)
            data = r.json()
            if r.status_code != 200 or "error" in data:
                error = data.get("error", {}).get("message", f"HTTP {r.status_code}")
                raise ValueError(error)
        except Exception as e:
            print(f"[LINK CHECK BLOCKED] {post_id}: {e}; skip story until permission/API recovers")
            send_error_alert(f"comment_link_check:{page_id}",
                             f"Page {page_id}: không xác minh được link trong bình luận ({e}). Tạm bỏ qua tạo truyện để tránh tạo lại bài đã gắn link.")
            return None
        for comment in data.get("data", []):
            msg = comment.get("message", "") or ""
            from_id = str((comment.get("from") or {}).get("id") or "")
            if contains_article_link(msg) or (from_id == str(page_id) and URL_PATTERN.search(msg)):
                print(f"[LINK] {post_id}: article/Page URL detected; skip")
                return True
        url = data.get("paging", {}).get("next")
        params = None
        pages_checked += 1
    # If comments were truncated, absence of link has not been established.
    if url:
        print(f"[LINK CHECK INCOMPLETE] {post_id}: reached {COMMENT_LINK_MAX_PAGES} pages; skip")
        return None
    return False


LINKED_POSTS_FILE = "/data/linked_posts.json"
def _load_linked_posts():
    try:
        with open(LINKED_POSTS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (OSError, ValueError, TypeError):
        return set()
linked_posts = _load_linked_posts()
def mark_linked_post(post_id):
    linked_posts.add(str(post_id))
    try:
        os.makedirs(os.path.dirname(LINKED_POSTS_FILE), exist_ok=True)
        with open(LINKED_POSTS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(linked_posts), f)
    except OSError as e:
        print(f"[WARN] Cannot persist linked post: {e}")

STORY_QUEUE = queue.Queue()
_story_pending = set()
_story_lock = threading.Lock()

def enqueue_story(page_name, post_id, message, page_id, page_token, alert_text, notified_key, created_at):
    with _story_lock:
        if post_id in _story_pending or post_id in story_completed or str(post_id) in linked_posts:
            return False
        _story_pending.add(post_id)
    STORY_QUEUE.put((page_name, post_id, message, page_id, page_token, alert_text, notified_key, created_at))
    return True

def story_worker():
    while True:
        page_name, post_id, message, page_id, page_token, alert_text, notified_key, created_at = STORY_QUEUE.get()
        try:
            if post_id in story_completed or str(post_id) in linked_posts:
                continue
            # Bài có thể đã quá 120 giờ trong lúc chờ hàng đợi OpenAI.
            if not is_post_within_window(created_at):
                print(f"[SKIP OLD QUEUED] {post_id}: bài đã quá {ONLY_POSTS_NEWER_THAN_HOURS} giờ")
                continue
            # Đợi đến lượt gửi cả nhóm; scan vẫn chạy mỗi 5 phút.
            with TELEGRAM_GROUP_LOCK:
                if not is_post_within_window(created_at):
                    print(f"[SKIP OLD BEFORE SEND] {post_id}")
                    continue
                link_status = post_already_has_link(post_id, page_id, page_token, message)
                if link_status is True:
                    mark_linked_post(post_id)
                if link_status is not False:
                    print(f"[WORKER] {post_id}: link exists or verification unavailable; skip TXT.")
                    continue
                if notified_key not in already_notified:
                    send_telegram_message(alert_text)
                    print(f"[TELEGRAM] post={post_id} alert_at={datetime.now(timezone.utc).isoformat()}")
                    already_notified.add(notified_key)
                    save_notified(already_notified)
                else:
                    print(f"[WORKER] {post_id}: already alerted; retry story without duplicate alert")
                # Khóa được giữ xuyên suốt 3 TXT: không tin Telegram nào từ
                # cùng tiến trình này có thể chen vào giữa nhóm bài.
                generate_and_send_story_continuation(page_name, post_id, message)
        except Exception as exc:
            print(f"[WORKER] Bài {post_id} lỗi: {exc}")
            send_error_alert(f"worker_{post_id}", f"Bài {post_id}: {exc}")
        finally:
            with _story_lock:
                _story_pending.discard(post_id)
            STORY_QUEUE.task_done()

def is_post_within_window(created_at, now_utc=None):
    """Fail closed: thiếu/sai ngày đăng hoặc bài quá 120 giờ đều không được báo."""
    try:
        published = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        if published.tzinfo is None:
            return False
        now_utc = now_utc or datetime.now(timezone.utc)
        age = (now_utc - published.astimezone(timezone.utc)).total_seconds()
        return 0 <= age <= ONLY_POSTS_NEWER_THAN_HOURS * 3600
    except (ValueError, TypeError, OverflowError):
        return False


def _scan_page(page, now_mono, now_utc):
    page_id, page_name, page_token = page["id"], page["name"], page["access_token"]
    tracked = tracked_posts.setdefault(str(page_id), {})
    cutoff = now_utc - timedelta(hours=ONLY_POSTS_NEWER_THAN_HOURS)
    for pid, item in list(tracked.items()):
        try:
            created = datetime.fromisoformat(item["created"])
            if not is_post_within_window(item["created"], now_utc) or pid in story_completed:
                del tracked[pid]
        except (ValueError, KeyError, TypeError):
            del tracked[pid]

    # Full discovery every 5 min, while fresh posts are discovered every minute.
    full = now_mono - _last_discovery.get((page_id, "full"), -1e12) >= FULL_POST_DISCOVERY_INTERVAL_SECONDS
    if full:
        discovered = get_recent_post_ids(page_id, page_token)
        _last_discovery[(page_id, "full")] = now_mono
    else:
        discovered = get_recent_post_ids_fast(page_id, page_token)
    for pid, created in discovered:
        if pid not in story_completed and is_post_within_window(created, now_utc):
            tracked.setdefault(pid, {"created": created, "last_check": 0, "near": False})

    candidates = []
    for pid, item in tracked.items():
        try:
            age = (now_utc - datetime.fromisoformat(item["created"])).total_seconds()
        except (ValueError, KeyError, TypeError):
            continue
        if not is_post_within_window(item["created"], now_utc):
            continue
        interval = CHECK_INTERVAL_SECONDS if age <= FAST_POST_AGE_HOURS * 3600 or item.get("near") else SLOW_POST_CHECK_INTERVAL_SECONDS
        if now_mono - item.get("last_check", 0) >= interval:
            candidates.append(pid)
    # Prioritize recently published and near-threshold posts; limit fast scan work.
    candidates.sort(key=lambda pid: (not tracked[pid].get("near", False), -datetime.fromisoformat(tracked[pid]["created"]).timestamp()))
    if not candidates:
        return
    stats = get_stats_batch(candidates, page_token)
    ts = datetime.now().strftime("%H:%M:%S")
    for pid in candidates:
        if pid not in stats:
            continue  # retry failed batch next scan
        views, comments, link, message = stats[pid]
        if comments is None or (views is None and comments <= COMMENT_ONLY_THRESHOLD):
            continue
        item = tracked[pid]
        if not is_post_within_window(item["created"]):
            print(f"[SKIP OLD BEFORE ALERT] {pid}")
            continue
        item["last_check"] = now_mono
        item["near"] = near_threshold(views or 0, comments)
        if not meets_threshold(views or 0, comments):
            continue
        key = f"{page_id}_{pid}"
        if pid in story_completed or str(pid) in linked_posts:
            continue
        if pid in _story_pending:
            continue
        link_status = post_already_has_link(pid, page_id, page_token, message)
        if link_status is not False:
            if link_status is True:
                mark_linked_post(pid)
                already_notified.add(key)
                save_notified(already_notified)
            continue  # None: retry verification later, do not generate or notify
        alert = (f"🔥 BÀI ĐANG LÊN! (Page: {page_name})\nViews: {views}\nComments: {comments}\n"
                 + (f"Link: {link}\n" if link else "")
                 + "=> Gắn link ngay!\n⏳ Đang tạo file TXT Part 2, 3 & 4 cho bài này...")
        print(f"[DETECTED] post={pid} page={page_name} api_threshold_at={datetime.now(timezone.utc).isoformat()} "
              f"published_at={item['created']} views={views} comments={comments} queue={STORY_QUEUE.qsize()}")
        enqueue_story(page_name, pid, message, page_id, page_token, alert, key, item["created"])


def get_recent_post_ids_fast(page_id, page_token):
    """Fetch first page each minute; deep pagination runs every 5 minutes."""
    try:
        r = api_get(f"{GRAPH_URL}/{page_id}/posts",
                    params={"fields": "id,created_time", "limit": POSTS_PAGE_LIMIT, "access_token": page_token})
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise ValueError(data["error"].get("message", "Graph API error"))
        result = []
        for post in data.get("data", []):
            pid, created = post.get("id"), post.get("created_time")
            if pid and is_post_within_window(created):
                result.append((pid, created))
        return result
    except Exception as e:
        print(f"[FAST DISCOVERY] Page {page_id}: {e}")
        return []


def check_all_pages():
    global _last_page_discovery, _cached_pages
    started = time.monotonic()
    if not hasattr(check_all_pages, "_last_token_check") or time.time() - check_all_pages._last_token_check > 86400:
        check_token_expiry()
        check_all_pages._last_token_check = time.time()
    if not _cached_pages or started - _last_page_discovery >= PAGE_DISCOVERY_INTERVAL_SECONDS:
        pages = get_managed_pages()
        if pages:
            _cached_pages = pages
            _last_page_discovery = started
    if not _cached_pages:
        print("[SCAN] Không có Page nào để quét")
        return
    now_utc = datetime.now(timezone.utc)
    with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
        jobs = [executor.submit(_scan_page, page, started, now_utc) for page in _cached_pages]
        for job in as_completed(jobs):
            try:
                job.result()
            except Exception as e:
                print(f"[SCAN] Page scan error: {e}")
    save_tracked_posts()
    print(f"[SCAN] completed_at={datetime.now(timezone.utc).isoformat()} elapsed={time.monotonic()-started:.1f}s "
          f"pages={len(_cached_pages)} queued={STORY_QUEUE.qsize()}")


def main():
    print("Bắt đầu theo dõi bài viết trên tất cả các Page... (Ctrl+C để dừng)")
    threading.Thread(target=story_worker, name="story-worker", daemon=True).start()
    while True:
        scan_started = time.monotonic()
        try:
            check_all_pages()
        except Exception as e:
            print(f"[LỖI] Lỗi không xác định trong vòng quét: {e}")
            send_error_alert("main_loop_exception", f"Lỗi không xác định trong vòng quét:\n{e}")
        time.sleep(max(0, CHECK_INTERVAL_SECONDS - (time.monotonic() - scan_started)))


if __name__ == "__main__":
    main()
