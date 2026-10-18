"""
FB POST MONITOR (RETRY + BATCH API + CẢNH BÁO TOKEN + AUTO PART 2/3)
=================================================================
Theo dõi tất cả (hoặc 1 phần) Page Facebook bạn quản lý. Gửi thông báo
Telegram khi 1 bài đạt ngưỡng (OR nhiều điều kiện) HOẶC có dấu hiệu
tăng đột biến ("dựng đứng"). Bỏ qua bài đã có link. Tự báo lỗi qua
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
import hashlib
import unicodedata
import html
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone, timedelta

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")


# ---------- Danh sách website của team (chỉ dùng để nhận diện link bài đọc) ----------
# WEB_SITES: mỗi dòng "link API: token" (ngăn cách bằng xuống dòng hoặc dấu ;). Có thể bỏ trống.
def _parse_web_sites(raw):
    sites = {}
    for item in re.split(r"[\n;]+", raw or ""):
        item = item.strip()
        if ":" not in item:
            continue
        url, _, token = item.rpartition(":")  # token không chứa dấu ":"
        url, token = url.strip().rstrip("/"), token.strip()
        if url.lower().endswith("/api"):
            url = url[:-4].rstrip("/")
        host = (urlparse(url).hostname or "").lower()
        if host and token and url.lower().startswith("https://"):
            sites[host] = {"base": url, "token": token}
    return sites


WEB_SITES = _parse_web_sites(os.getenv("WEB_SITES", ""))


def _resolve_web_base_url():
    """Muốn đổi web chỉ cần sửa WEB_API_URL (hoặc WEB_BASE_URL); tự bỏ hậu tố /api."""
    raw = (os.getenv("WEB_API_URL") or os.getenv("WEB_BASE_URL") or "https://puretales.idolsgift.com").strip().rstrip("/")
    if "://" not in raw:
        raw = "https://" + raw
    if raw.lower().endswith("/api"):
        raw = raw[:-4].rstrip("/")
    return raw


_WEB_BASE_URL_RESOLVED = _resolve_web_base_url()

# Domain link bài đọc: tự gồm mọi website trong WEB_SITES + website đang dùng + ARTICLE_LINK_DOMAINS.
# Không còn crash khi chưa đặt biến ARTICLE_LINK_DOMAINS.
ARTICLE_LINK_DOMAINS = sorted(set(
    list(WEB_SITES.keys())
    + ([(urlparse(_WEB_BASE_URL_RESOLVED).hostname or "").lower()] if urlparse(_WEB_BASE_URL_RESOLVED).hostname else [])
    + [d.strip().lower() for d in os.getenv(
        "ARTICLE_LINK_DOMAINS", "storyly.chosouthrussianovcharka.com").split(",") if d.strip()]
))
COMMENT_LINK_MAX_PAGES = 5  # quét tối đa 5 trang x 100 comments trước khi báo / gọi OpenAI
COMMENT_COUNT_MAX_PAGES = 10  # fallback đếm comment thực tế khi summary có thể thấp hơn UI
COMMENT_COUNT_NEAR_THRESHOLD = 15  # chỉ deep-count khi còn cách mốc comment <= 15 để tiết kiệm Graph API

# --- Tự viết Part 2 + Part 3 + Part 4 bằng 3 OpenAI API request liên tiếp ---
ENABLE_STORY_CONTINUATION = True
OPENAI_MODEL = "gpt-5.6-luna"
OPENAI_MAX_OUTPUT_TOKENS = 5000  # cost-control: mục tiêu 2500-3000 từ/Part
OPENAI_TIMEOUT_SECONDS = 600
OPENAI_PART_RETRIES = 1  # Không tự lặp request có thể đã bị tính phí  # chỉ retry lỗi API/network/content rỗng; không regenerate nội dung ngắn

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
• Length: target approximately 2500-3000 words for this part. Aim tightly for about 2600-2800 words. NEVER finish below 2500 words. Keep the story concise and naturally complete; avoid filler, repeated explanations, recaps, and unnecessary extra scenes.
• Maintain strict consistency in characters, settings, timeline, facts, relationships, and unresolved clues from all story context supplied above.
• Do NOT repeat scenes or recap large portions unnecessarily. Continue naturally from the exact point where the previous part ended.
• IMPORTANT: Do NOT write any END OF PART line, NEXT PART line, Facebook CTA, like/share request, or other ending marker. The program will append the correct ending line only after the part has been fully generated.
"""

PART_ENDINGS = {
    2: 'END OF PART 2 — THE STORY CONTINUES IN PART 3. GO BACK TO THE COMMENTS AND TAP THE PART 3 LINK TO KEEP READING.',
    3: '-END OF PART 3–PRESS NEX PART IN BOTTOM OF PAGE TO READ NEXT PART',
    4: '- END OF PART 4 ​​- LET SAY YES AND LIKE, SHARE THIS POST IN FACEBOOK SO THAT WE HAVE THE MOTIVATION TO SHARE MORE STORIES',
}

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Website: 
ENABLE_WEB_PUBLISH_TEST = os.getenv("ENABLE_WEB_PUBLISH_TEST", "true").lower() == "true"
# Bật/tắt đăng web bằng Railway: ENABLE_WEB_PUBLISH=true (tên cũ ENABLE_AUTO_WEB_PUBLISH vẫn dùng được).
ENABLE_AUTO_WEB_PUBLISH = os.getenv("ENABLE_WEB_PUBLISH", os.getenv("ENABLE_AUTO_WEB_PUBLISH", "false")).strip().lower() in ("1", "true", "yes", "on")
WEB_BASE_URL = _WEB_BASE_URL_RESOLVED  # lấy từ WEB_API_URL hoặc WEB_BASE_URL
# Token API của website đang dùng: tự lấy trong WEB_SITES theo domain của WEB_API_URL.
_ACTIVE_WEB_HOST = (urlparse(WEB_BASE_URL).hostname or "").lower()
BIOBLOG_API_TOKEN = (WEB_SITES.get(_ACTIVE_WEB_HOST) or {}).get("token") or os.getenv("BIOBLOG_API_TOKEN", "").strip()
if ENABLE_AUTO_WEB_PUBLISH and not BIOBLOG_API_TOKEN:
    print(f"[CONFIG-WARN] Không tìm thấy token cho website {_ACTIVE_WEB_HOST!r}: thêm dòng 'link: token' vào WEB_SITES.", flush=True)
# WEB_PUBLISH_PAGES="all" (mặc định) = mọi Page; hoặc liệt kê Page ID / tên Page, ngăn cách bằng dấu phẩy.
_web_publish_raw = os.getenv("WEB_PUBLISH_PAGES", "all").strip()
WEB_PUBLISH_ALL_PAGES = _web_publish_raw.lower() in ("", "all", "*")
WEB_PUBLISH_PAGE_KEYS = set() if WEB_PUBLISH_ALL_PAGES else {
    x.strip().casefold() for x in re.split(r"[,;\n]+", _web_publish_raw) if x.strip()}
# Mã Page (KHÔNG BẮT BUỘC) — ai muốn đánh số để dễ nhận biết thì khai trong env:
#   PAGE_NUMBERS="Tên Page A=1;Tên Page B=2;..."
# Page có mã: tiêu đề "PART 2: Tên truyện15 001". Page không có mã: "PART 2: Tên truyện 001".
PAGE_NUMBER_BY_NAME = {}
for _item in re.split(r"[;\n]+", os.getenv("PAGE_NUMBERS", "")):
    _pname, _, _pnum = _item.rpartition("=")
    if _pname.strip() and _pnum.strip().isdigit():
        PAGE_NUMBER_BY_NAME[re.sub(r"\s+", " ", _pname).strip().casefold()] = int(_pnum.strip())


def _page_number_or_none(page_name):
    """Trả mã Page theo tên (không phân biệt hoa thường/khoảng trắng thừa); không có thì None."""
    return PAGE_NUMBER_BY_NAME.get(re.sub(r"\s+", " ", str(page_name or "")).strip().casefold())


# Bài cũ đã gửi đủ 3 TXT trong N giờ gần đây nhưng chưa lên web sẽ được đăng bù khi bật lại.
WEB_BACKFILL_HOURS = float(os.getenv("WEB_BACKFILL_HOURS", "168"))
PUBLISH_PLATFORM = os.getenv("PUBLISH_PLATFORM", "bioblog").strip().lower()
if PUBLISH_PLATFORM not in ("bioblog", "wordpress"):
    raise ValueError("PUBLISH_PLATFORM phải là bioblog hoặc wordpress")
WP_BASE_URL = os.getenv("WP_BASE_URL", "https://storyly.chosouthrussianovcharka.com").rstrip("/")
WP_USERNAME = os.getenv("WP_USERNAME", "")
WP_APPLICATION_PASSWORD = os.getenv("WP_APPLICATION_PASSWORD", "")
WP_POSTS_PUBLIC = os.getenv("WP_POSTS_PUBLIC", "false").lower() == "true"
WEB_ADMIN_EMAIL = os.getenv("WEB_ADMIN_EMAIL", "")
WEB_ADMIN_PASSWORD = os.getenv("WEB_ADMIN_PASSWORD", "")
# Đường dẫn login tùy cấu hình website; cần xác minh trước khi bật publish.
WEB_LOGIN_PATH = os.getenv("WEB_LOGIN_PATH", "/login")
WEB_LOGIN_EMAIL_FIELD = os.getenv("WEB_LOGIN_EMAIL_FIELD", "email")
WEB_LOGIN_PASSWORD_FIELD = os.getenv("WEB_LOGIN_PASSWORD_FIELD", "password")
WEB_POST_IMAGE_URL = os.getenv("WEB_POST_IMAGE_URL", "")  # Chỉ fallback khi WEB_ALLOW_IMAGE_FALLBACK=true
WEB_ALLOW_IMAGE_FALLBACK = os.getenv("WEB_ALLOW_IMAGE_FALLBACK", "false").lower() == "true"
WEB_IMAGE_MAX_BYTES = 15 * 1024 * 1024
WEB_POSTS_PUBLIC = os.getenv("WEB_POSTS_PUBLIC", "true").lower() == "true"
WEB_UPDATE_METHOD = "PUT"
AUTHOR_CODE = os.getenv("AUTHOR_CODE", "").strip()
if not re.fullmatch(r"[0-9A-Za-z_-]{1,16}", AUTHOR_CODE):
    raise ValueError("AUTHOR_CODE không hợp lệ (thiếu biến AUTHOR_CODE hoặc chứa ký tự lạ)")
# author gửi lên API phải là USERNAME tài khoản của bạn trên web (cột Username, không có @).
# Nếu sai, bài bị gán cho tài khoản chủ token và bạn sẽ không thấy bài trong tài khoản của mình.
AUTHOR_USERNAME = (os.getenv("AUTHOR_USERNAME", "").strip().lstrip("@") or AUTHOR_CODE)
if not re.fullmatch(r"[0-9A-Za-z_.-]{1,40}", AUTHOR_USERNAME):
    raise ValueError("AUTHOR_USERNAME không hợp lệ")
PUBLISH_APPROVAL_POLL_SECONDS = 8
ENABLE_STORY_CACHE = os.getenv("ENABLE_STORY_CACHE", "true").lower() == "true"
STORY_CACHE_DIR = "/data/story_cache"


# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
THRESHOLD_RULES = [
    {"min_views": 6000, "min_comments": 20},
    {"min_views": 3500, "min_comments": 70},
]
COMMENT_ONLY_THRESHOLD = 100  # comments vượt mốc này thì báo luôn, không cần xét views

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 120

# --- Phát hiện "dựng đứng" (viral spike) dựa trên tốc độ tăng views ---
# Railway Variable: ENABLE_SPIKE_ALERT=true để BẬT, false để TẮT (mặc định tắt).
ENABLE_SPIKE_ALERT = os.getenv("ENABLE_SPIKE_ALERT", "false").strip().lower() in ("1", "true", "yes", "on")
SPIKE_LOOKBACK_MINUTES = 30  # mốc tham chiếu; baseline thực tế chỉ lấy trong 15–40 phút
SPIKE_MIN_VIEW_INCREASE = 3000
SPIKE_MIN_PERCENT_INCREASE = 80
SPIKE_MIN_VIEWS_TO_CHECK = 2000
VIEW_HISTORY_FILE = "/data/view_history.json"
STORY_COMPLETED_FILE = "/data/story_completed_posts.json"
STORY_CLAIMED_FILE = "/data/story_claimed_posts.json"
STORY_PROGRESS_DIR = "/data/story_progress"
STORY_RETRY_SECONDS = 60  # retry checkpoint nhanh, không regenerate Part đã lưu
ERROR_ONCE_FILE = "/data/error_once_keys.json"
ENABLE_PAID_GENERATION = True  # chỉ đánh dấu sau khi TXT đủ Part 2+3+4 đã gửi thành công

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

CHECK_INTERVAL_SECONDS = 60  # vòng quét nhanh; từng bài được adaptive polling để giảm tải Graph API
POST_POLL_NEAR_SECONDS = 60
POST_POLL_WARM_SECONDS = 120
POST_POLL_COLD_SECONDS = 300
MANAGED_PAGES_CACHE_SECONDS = 600
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

def _load_keys(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (OSError, ValueError, TypeError):
        return set()

def _save_keys(path, keys):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(sorted(keys), f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)

story_claimed = _load_keys(STORY_CLAIMED_FILE)
error_once_keys = _load_keys(ERROR_ONCE_FILE)

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


# Thông tin lần đo gần nhất, chỉ dùng để giải thích cảnh báo; không ảnh hưởng checkpoint.
spike_evidence = {}


def record_and_check_spike(post_id: str, current_views: int, now: datetime):
    """Phát hiện tăng views nhanh trong cửa sổ THỰC 15–40 phút.

    Chế độ cân bằng: kết hợp tăng tuyệt đối, tỷ lệ và tốc độ/phút; nếu có
    lịch sử giai đoạn trước thì yêu cầu tăng tốc để giảm cảnh báo sai.
    """
    post_id = str(post_id)
    current_views = int(current_views)
    spike_evidence.pop(post_id, None)
    raw = view_history.get(post_id, [])
    samples = []
    for entry in raw:
        try:
            stamp = datetime.fromisoformat(entry[0])
            if stamp.tzinfo is None and now.tzinfo is not None:
                stamp = stamp.replace(tzinfo=now.tzinfo)
            if stamp.tzinfo is not None and now.tzinfo is None:
                stamp = stamp.replace(tzinfo=None)
            value = int(entry[1])
            age = (now - stamp).total_seconds() / 60
            if 0 <= age <= 90 and value >= 0:
                samples.append((stamp, value, age))
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    samples.sort(key=lambda item: item[0])

    # Luôn ghi mẫu mới kể cả khi cảnh báo tắt, để có baseline khi bật lại.
    samples.append((now, current_views, 0.0))
    view_history[post_id] = [[stamp.isoformat(), value] for stamp, value, _ in samples[-200:]]

    if current_views < SPIKE_MIN_VIEWS_TO_CHECK:
        return False

    # Chọn mẫu gần mốc 25 phút nhất, nhưng tuyệt đối không cũ hơn 40 phút.
    candidates = [sample for sample in samples[:-1] if 15 <= sample[2] <= 40]
    if not candidates:
        return False
    base_time, base_views, minutes = min(candidates, key=lambda sample: abs(sample[2] - 25))
    increase = current_views - base_views
    if increase <= 0:
        return False
    pct = (100.0 * increase / base_views) if base_views > 0 else 0.0
    rate = increase / minutes

    # Hai nhánh: tăng tương đối rõ rệt, hoặc tăng tuyệt đối rất mạnh.
    strong_relative = increase >= 1200 and pct >= 65 and rate >= 40
    strong_absolute = increase >= 2500 and rate >= 55
    if not (strong_relative or strong_absolute):
        return False

    # Có dữ liệu giai đoạn trước: yêu cầu tốc độ hiện tại cao hơn ít nhất 20%.
    earlier = [sample for sample in samples[:-1]
               if 15 <= (base_time - sample[0]).total_seconds() / 60 <= 40]
    if earlier:
        old_time, old_views, _ = min(
            earlier, key=lambda sample: abs((base_time - sample[0]).total_seconds() / 60 - 25)
        )
        previous_minutes = (base_time - old_time).total_seconds() / 60
        previous_rate = max(0.0, (base_views - old_views) / previous_minutes)
        if previous_rate > 0 and rate < previous_rate * 1.20:
            return False

    # Nếu có mẫu rất gần đây, tránh báo khi views đã chững lại.
    recent = [sample for sample in samples[:-1] if 4 <= sample[2] <= 12]
    if recent:
        _, recent_views, recent_minutes = min(recent, key=lambda sample: abs(sample[2] - 8))
        recent_rate = (current_views - recent_views) / recent_minutes
        if recent_rate < max(20.0, rate * 0.45):
            return False

    spike_evidence[post_id] = {
        "baseline_views": base_views,
        "current_views": current_views,
        "minutes": round(minutes, 1),
        "increase": increase,
        "percent": round(pct, 1),
        "rate": round(rate, 1),
    }
    return True


# -------------------- Telegram --------------------
TELEGRAM_GROUP_LOCK = threading.RLock()  # Giữ nguyên yêu cầu: không cho tin khác chen giữa thông báo và các TXT của chính bài đó.

def _send_telegram_message_unlocked(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = api_post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        if resp.status_code != 200:
            print(f"[LỖI] Gửi Telegram thất bại: {resp.text}")
            return False
        return bool(resp.json().get("ok"))
    except Exception as e:
        print(f"[LỖI] Gửi Telegram thất bại (mạng): {e}")
        return False


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
            if resp.status_code == 200 and resp.json().get("ok"):
                resolve_error_alert("telegram_document_failed", "Gui file TXT Telegram da hoat dong lai.")
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
    # Bình thường chỉ 6000 tokens. Chỉ cứu đúng trường hợp reasoning ăn hết budget
    # làm content rỗng + finish_reason=length; không retry content đã trả về.
    token_budget = OPENAI_MAX_OUTPUT_TOKENS
    total_attempts = 2
    for attempt in range(1, total_attempts + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": full_prompt}],
                    "max_completion_tokens": token_budget,
                    "reasoning_effort": "none",
                },
                timeout=OPENAI_TIMEOUT_SECONDS,
            )
            try:
                data = resp.json()
            except Exception:
                data = {}

            if resp.status_code != 200:
                last_error = data.get("error", {}).get("message", resp.text[:1000])
                print(f"[CẢNH BÁO] Part {part_number} HTTP {resp.status_code}: {last_error}")
                break

            choices = data.get("choices") or []
            if not choices:
                last_error = f"không có choices; response={str(data)[:1200]}"
                break

            choice = choices[0] or {}
            msg = choice.get("message") or {}
            content = msg.get("content") or ""
            if isinstance(content, list):
                content = "".join(x.get("text", "") if isinstance(x, dict) else str(x) for x in content)
            content = str(content).strip()
            finish_reason = choice.get("finish_reason")
            usage = data.get("usage", {})

            if content:
                resolve_error_alert(f"openai_api_part_{part_number}_failed", f"OpenAI Part {part_number} da tra noi dung thanh cong.")
                print(f"[OPENAI] Part {part_number} OK | finish_reason={finish_reason} | usage={usage}")
                return content

            last_error = f"content rỗng; finish_reason={finish_reason}; usage={usage}"
            print(f"[CẢNH BÁO] Part {part_number}: {last_error}")

            if attempt == 1 and finish_reason == "length":
                token_budget = 7000
                print(f"[OPENAI] Part {part_number}: content rỗng do length -> cứu 1 lần với 7000 tokens.")
                continue
            break

        except requests.exceptions.RequestException as e:
            last_error = f"network/timeout: {e}"
            print(f"[CẢNH BÁO] Part {part_number}: {last_error}")
            break
        except Exception as e:
            last_error = f"parse/process: {e}"
            print(f"[CẢNH BÁO] Part {part_number}: {last_error}")
            break

    send_error_alert(
        f"openai_api_part_{part_number}_failed",
        f"Không tạo được Part {part_number}. Không regenerate content đã trả về. Chi tiết cuối: {last_error}",
    )
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
    # Mỗi Part bắt buộc tối thiểu 2500 từ.
    if words < 2500:
        return False, f"quá ngắn ({words} từ; bắt buộc tối thiểu 2500 từ)"
    if part_number in (3, 4) and not re.search(rf"(?i)PART\s+{part_number}", text):
        return False, f"thiếu nhãn PART {part_number}"
    return True, f"OK ({words} từ)"


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w’'-]+\b", text or "", flags=re.UNICODE))


def call_openai_continue_part(story_context: str, existing_part: str, part_number: int, target_words: int = 2650):
    """Bổ sung phần còn thiếu thay vì vứt content đã trả tiền và generate lại từ đầu."""
    current_words = _word_count(existing_part)
    need_words = max(0, target_words - current_words)
    # Chỉ mua đúng phần còn thiếu; dư rất nhẹ để kết thúc tự nhiên.
    requested_words = min(max(need_words + 50, 180), 750)

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
Continue PART {part_number} from the exact final sentence above. Write ONLY approximately {requested_words} additional words so the COMPLETE part reaches 2500-3000 words, ideally near 2650 words. Stop promptly; do not add filler.
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
                    # Continuation chỉ mua phần còn thiếu để đạt tối thiểu 2500 từ.
                    "max_completion_tokens": min(2200, max(900, int(requested_words * 1.8))),
                    "reasoning_effort": "none",
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

    # Nếu chưa đủ 2500 từ, tuyệt đối không regenerate toàn bộ.
    # Giữ content và gọi continuation để đạt tối thiểu 2500 từ.
    continuation_rounds = 1
    while words < 2500 and continuation_rounds > 0:
        print(f"[OPENAI] Part {part_number} mới có {words} từ -> giữ nguyên và viết bổ sung, KHÔNG regenerate.")
        extra = call_openai_continue_part(story_context, raw, part_number, target_words=2650)
        if not extra:
            break
        raw = f"{raw.rstrip()}\n\n{extra.strip()}"
        raw = _strip_program_endings(raw)
        words = _word_count(raw)
        continuation_rounds -= 1

    ok, reason = _validate_part(raw, part_number)
    if ok:
        resolve_error_alert(f"openai_part_{part_number}_generation_failed", f"Da tao lai duoc Part {part_number}.")
        resolve_error_alert(f"openai_part_{part_number}_validation_failed", f"Part {part_number} da dat kiem tra noi dung.")
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


def _progress_path(post_id):
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", str(post_id))
    return os.path.join(STORY_PROGRESS_DIR, safe + ".json")


def _save_progress(post_id, progress):
    os.makedirs(STORY_PROGRESS_DIR, exist_ok=True)
    path = _progress_path(post_id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_progress(post_id):
    try:
        with open(_progress_path(post_id), "r", encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _retry_due(post_id):
    state = _load_progress(post_id)
    return time.time() >= float(state.get("retry_after", 0))


# -------------------- Kho truyện (tái sử dụng Part 2/3/4 khi trùng caption) --------------------
def _normalized_caption(caption):
    """Chỉ loại khác biệt Unicode và khoảng trắng; không ghép truyện gần giống."""
    return " ".join(unicodedata.normalize("NFC", caption or "").split())


def _cache_key(caption):
    return hashlib.sha256(json.dumps({"caption": _normalized_caption(caption), "model": OPENAI_MODEL, "rules": STORY_WRITING_RULES, "v": 2}, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _cache_path(caption):
    return os.path.join(STORY_CACHE_DIR, _cache_key(caption) + ".json")


def _valid_cached_parts(parts):
    return isinstance(parts, dict) and all(
        isinstance(parts.get(str(n)), str) and _validate_part(parts[str(n)], n)[0]
        for n in (2, 3, 4)
    )


def _cache_read(caption):
    if not ENABLE_STORY_CACHE:
        return None
    path = _cache_path(caption)
    try:
        with open(path, encoding="utf-8") as f:
            entry = json.load(f)
        if entry.get("caption_normalized") == _normalized_caption(caption) and _valid_cached_parts(entry.get("parts")):
            return dict(entry["parts"])
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    # Tận dụng checkpoint cũ đã hoàn thành (không mua lại API).
    if os.path.isdir(STORY_PROGRESS_DIR):
        for filename in os.listdir(STORY_PROGRESS_DIR):
            if not re.fullmatch(r"[A-Za-z0-9_]+\.json", filename):
                continue
            try:
                with open(os.path.join(STORY_PROGRESS_DIR, filename), encoding="utf-8") as f:
                    state = json.load(f)
                if (_normalized_caption(state.get("caption", "")) == _normalized_caption(caption)
                        and _valid_cached_parts(state.get("parts"))):
                    _cache_write(caption, state["parts"])
                    return dict(state["parts"])
            except (OSError, ValueError, TypeError, AttributeError):
                continue
    return None


def _cache_write(caption, parts):
    if not ENABLE_STORY_CACHE or not _valid_cached_parts(parts):
        return
    os.makedirs(STORY_CACHE_DIR, exist_ok=True)
    path = _cache_path(caption)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump({"caption_normalized": _normalized_caption(caption), "parts": parts}, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def _resume_story(page_name, post_id, caption):
    """Persist each successful part; only generate missing parts. Never re-buy saved parts."""
    state = _load_progress(post_id)
    if state.get("caption") and state["caption"] != caption:
        # A changed caption cannot safely be combined with already-generated parts.
        send_error_alert("caption_changed_" + str(post_id), f"Bài {post_id}: caption đã thay đổi; giữ bản đã lưu, không tạo lại để tránh phí trùng.")
        return False
    if not state.get("caption"):
        # Checkpoint có thể đã được worker tạo trước (chỉ có cờ auto_publish_eligible/page_name):
        # vẫn phải ghi caption trước khi gọi bất kỳ request trả phí nào.
        state.setdefault("parts", {})
        state.setdefault("sent", [])
        state.setdefault("retry_after", 0)
        state.setdefault("auto_publish_eligible", False)
        state["caption"] = caption
        state["page_name"] = state.get("page_name") or page_name
        _save_progress(post_id, state)  # fail closed before any paid request
    if not state.get("page_name"):
        state["page_name"] = page_name
        _save_progress(post_id, state)
    parts = state.setdefault("parts", {})
    # Chỉ áp dụng kho truyện nếu bài hiện tại chưa sinh phần nào.
    if not parts:
        cached = _cache_read(caption)
        if cached:
            parts.update(cached)
            state["cache_reused"] = True
            _save_progress(post_id, state)
            # Không gửi tin riêng (giữ đúng cấu trúc: bài đang lên -> 3 TXT -> 3 link); ghi chú vào caption PART 2.
            print(f"[CACHE] Bài {post_id}: tái sử dụng Part 2/3/4 từ kho truyện; không gọi OpenAI.", flush=True)
    for n in (2, 3, 4):
        key = str(n)
        if key in parts:
            valid, reason = _validate_part(parts[key], n)
            if not valid:
                raise ValueError(f"Part {n} đã lưu không hợp lệ ({reason}); không tự mua lại")
            continue
        context = caption + "".join("\n\n" + parts[str(k)] for k in range(2, n))
        print(f"[OPENAI] Bài {post_id}: tiếp tục Part {n}, giữ các Part đã lưu.")
        result = generate_valid_part(context, n)
        if not result:
            return False
        parts[key] = result
        _save_progress(post_id, state)  # atomic checkpoint immediately after success

    _cache_write(caption, parts)
    story = "\n\n".join((
        ("=" * 60 + "\n\n" if n > 2 else "") + parts[str(n)] + "\n\n**" + PART_ENDINGS[n] + "**"
        for n in (2, 3, 4)
    ))
    outputs = split_story_into_parts(story)
    sent = state.setdefault("sent", [])
    for n in (2, 3, 4):
        if n in sent:
            continue
        path = f"/tmp/story_{re.sub(r'[^a-zA-Z0-9_]', '_', str(post_id))}_PART_{n}.txt"
        try:
            create_part_txt(outputs[n], n, path)
            cap = f"📄 PART {n} — Page: {page_name}"
            if n == 2 and state.get("cache_reused"):
                cap += " (♻️ truyện từ kho, không gọi OpenAI)"
            if not send_telegram_document(path, caption=cap):
                return False
            sent.append(n)
            # Mốc "vừa gửi TXT": tin 3 link gửi ngay sau đó thì cả nhóm liền nhau.
            state["txt_group_at"] = time.time()
            _save_progress(post_id, state)
        finally:
            if os.path.exists(path):
                os.remove(path)
    # Đủ 3 TXT -> đăng web ngay (chỉ bài mới đã được worker xác nhận; bật bằng ENABLE_WEB_PUBLISH=true).
    # Lỗi đăng web không làm hỏng phần TXT: luồng khôi phục sẽ tự thử lại sau.
    if state.get("auto_publish_eligible") and ENABLE_AUTO_WEB_PUBLISH:
        try:
            _auto_publish_with_retry(post_id)
        except Exception as exc:
            print(f"[AUTO-WEB] {post_id}: {exc}", flush=True)
    story_completed.add(str(post_id))
    save_story_completed(story_completed)
    resolve_error_alert("story_resume_" + str(post_id), f"Bài {post_id} đã gửi đủ 3 file TXT.")
    resolve_error_alert(f"worker_{post_id}", f"Bài {post_id} đã xử lý xong.")
    print(f"[OPENAI] Bài {post_id}: đã gửi đủ Part 2/3/4, không tạo lại.")
    return True


def generate_and_send_story_continuation(page_name: str, post_id: str, caption: str):
    if not ENABLE_STORY_CONTINUATION or not ENABLE_PAID_GENERATION or not caption.strip():
        return False
    if str(post_id) in story_completed:
        return True
    if not _retry_due(post_id):
        return False
    try:
        # Old claimed keys are no longer permanent locks; checkpoint is authoritative.
        return _resume_story(page_name, post_id, caption)
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
        print(f"[STORY] {post_id}: {exc}")
        send_error_alert("story_resume_" + str(post_id), f"Bài {post_id}: tạm hoãn tiếp tục truyện: {exc}")
        return False
    finally:
        state = _load_progress(post_id)
        if state and str(post_id) not in story_completed:
            state["retry_after"] = time.time() + STORY_RETRY_SECONDS
            try:
                _save_progress(post_id, state)
            except OSError as exc:
                print(f"[STORY] Không lưu được lịch thử lại {post_id}: {exc}")


ERROR_STATUS_FILE = "/data/error_status_v2.json"
_ERROR_STATUS_LOCK = threading.RLock()

def _load_error_status():
    try:
        with open(ERROR_STATUS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}

_error_status = _load_error_status()

def _persist_error_status():
    os.makedirs(os.path.dirname(ERROR_STATUS_FILE), exist_ok=True)
    temp = ERROR_STATUS_FILE + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(_error_status, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, ERROR_STATUS_FILE)

def _mask_secrets(text):
    """Không bao giờ để lộ token/key trong tin Telegram hay log."""
    text = str(text)
    text = re.sub(r"(access_token=|input_token=)[^&\s\"']+", r"\1***", text)
    text = re.sub(r"(Bearer\s+)[A-Za-z0-9._\-]+", r"\1***", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_\-]{10,}", "sk-***", text)
    text = re.sub(r"\bEAA[A-Za-z0-9]{20,}", "EAA***", text)
    text = re.sub(r"bot\d{6,}:[A-Za-z0-9_\-]{20,}", "bot***", text)
    return text



# ==================== CHI TIẾT LỖI FACEBOOK ====================
_FB_ERROR_HINTS = {
    190: "Token hết hạn hoặc không còn hợp lệ — lấy token mới và cập nhật USER_ACCESS_TOKEN.",
    102: "Phiên đăng nhập Facebook hết hạn — lấy token mới.",
    10: "Thiếu quyền — token chưa được cấp quyền cần thiết cho Page này.",
    200: "Thiếu quyền — tài khoản có thể không còn quyền quản lý Page này.",
    100: "Facebook không cho đọc Page này — kiểm tra tài khoản còn quyền quản lý Page, Page có bị hạn chế/ẩn/xóa không.",
    368: "Page hoặc tài khoản đang bị Facebook tạm chặn tính năng.",
    4: "Gọi API quá nhiều (giới hạn của ứng dụng) — bot sẽ tự thử lại.",
    17: "Gọi API quá nhiều (giới hạn của tài khoản) — bot sẽ tự thử lại.",
    32: "Gọi API quá nhiều cho Page này — bot sẽ tự thử lại.",
    613: "Gọi API quá nhiều — bot sẽ tự thử lại.",
    80001: "Gọi API quá nhiều cho Page này — bot sẽ tự thử lại.",
}
_FB_SUBCODE_HINTS = {
    458: "Ứng dụng đã bị gỡ khỏi tài khoản.",
    459: "Tài khoản Facebook đang bị checkpoint — đăng nhập Facebook để xác minh.",
    460: "Mật khẩu Facebook đã đổi — lấy token mới.",
    463: "Token đã hết hạn — lấy token mới.",
    464: "Tài khoản chưa xác minh.",
    467: "Token không hợp lệ (đã đăng xuất hoặc bị thu hồi) — lấy token mới.",
    492: "Tài khoản không còn vai trò hợp lệ trên Page.",
}


def _fb_error_detail(resp=None, data=None):
    """Lấy lý do lỗi Facebook trả về (message, code, subcode) + gợi ý cách sửa bằng tiếng Việt."""
    if data is None and resp is not None:
        try:
            data = resp.json()
        except ValueError:
            data = None
    err = (data or {}).get("error") if isinstance(data, dict) else None
    if not isinstance(err, dict):
        status = getattr(resp, "status_code", "?")
        body = (getattr(resp, "text", "") or "")[:200]
        return f"HTTP {status}: {_mask_secrets(body) or 'không có nội dung'}"
    code, sub = err.get("code"), err.get("error_subcode")
    parts = [f"Facebook báo: {_mask_secrets(err.get('message', 'không rõ'))}"]
    parts.append(f"(code={code}" + (f", subcode={sub}" if sub else "") + (f", type={err.get('type')}" if err.get("type") else "") + ")")
    hint = _FB_SUBCODE_HINTS.get(sub) or _FB_ERROR_HINTS.get(code)
    if hint:
        parts.append(f"\n👉 Cách xử lý: {hint}")
    return " ".join(parts)


def _page_label(page_id):
    page = next((p for p in (_managed_pages_cache.get("pages") or []) if str(p.get("id")) == str(page_id)), None)
    return f"{page.get('name')} ({page_id})" if page and page.get("name") else str(page_id)


def send_error_alert(error_key: str, text: str):
    """Mỗi lỗi chỉ báo MỘT lần khi phát sinh (kể cả sau restart). Gửi Telegram NGOÀI khóa để không treo bot."""
    key = str(error_key)
    with _ERROR_STATUS_LOCK:
        if _error_status.get(key) in ("active", "sending"):
            print(f"[LỖI-ĐÃ-BÁO] {key}: {_mask_secrets(text)[:300]}")
            return False
        _error_status[key] = "sending"  # giữ chỗ, luồng khác không gửi trùng
    now = datetime.now()
    message = f"⚠️ LỖI SCRIPT!\n{_mask_secrets(text)}\n\nThời gian: {now.strftime('%H:%M:%S %d/%m/%Y')}"
    ok = send_telegram_message(message)
    with _ERROR_STATUS_LOCK:
        if ok:
            _error_status[key] = "active"
            _persist_error_status()
        else:
            _error_status.pop(key, None)
            print(f"[CẢNH BÁO] Gửi báo lỗi {key} chưa thành công; sẽ thử lại lần lỗi sau.")
    return bool(ok)


def resolve_error_alert(error_key: str, description: str = ""):
    """Gọi khi phần việc đó chạy lại bình thường: báo "ĐÃ KHẮC PHỤC" một lần; lỗi lại thì sẽ báo lại. Gửi ngoài khóa."""
    key = str(error_key)
    with _ERROR_STATUS_LOCK:
        if _error_status.get(key) != "active":
            return False
        _error_status[key] = "resolving"
    now = datetime.now()
    ok = send_telegram_message(
        f"✅ ĐÃ KHẮC PHỤC: {_mask_secrets(description or key)}\n\nThời gian: {now.strftime('%H:%M:%S %d/%m/%Y')}")
    with _ERROR_STATUS_LOCK:
        if ok:
            _error_status.pop(key, None)
        else:
            _error_status[key] = "active"
        _persist_error_status()
    return bool(ok)


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

    if data.get("is_valid", True):
        resolve_error_alert("token_invalid", "Facebook access token đã hợp lệ trở lại.")
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
    if days_left > TOKEN_EXPIRY_WARNING_DAYS:
        resolve_error_alert("token_expiry_warning", "Facebook token đã được gia hạn.")
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
    """Chỉ Telegram sau 3 vòng lỗi liên tiếp; lỗi thoáng qua chỉ log. Đã báo thì không báo lại tới khi sửa xong."""
    count = _fb_network_failures.get(error_key, 0) + 1
    _fb_network_failures[error_key] = count
    print(f"[FACEBOOK] {error_key}: lỗi liên tiếp {count}/{FB_NETWORK_ALERT_AFTER_FAILURES} - {_mask_secrets(text)}")
    if count >= FB_NETWORK_ALERT_AFTER_FAILURES:
        send_error_alert(error_key, f"Facebook lỗi {count} lần liên tiếp.\n{text}")


def note_fb_network_success(error_key: str, description: str = ""):
    """Request Facebook chạy lại được: reset bộ đếm và báo đã khắc phục (nếu lỗi từng được báo)."""
    had_failures = _fb_network_failures.get(error_key, 0)
    _fb_network_failures[error_key] = 0
    if had_failures:
        print(f"[FACEBOOK] {error_key}: đã phục hồi sau {had_failures} lần lỗi.")
    resolve_error_alert(error_key, description or f"Facebook ({error_key}) đã chạy lại bình thường.")

# -------------------- Facebook API --------------------
_managed_pages_cache = {"at": 0.0, "pages": []}
_post_next_check = {}


def _adaptive_poll_seconds(views: int, comments: int) -> int:
    """Bài càng gần ngưỡng càng được kiểm tra nhanh; bài xa ngưỡng giảm tần suất để tiết kiệm Graph API."""
    views = int(views or 0)
    comments = int(comments or 0)
    if views >= 2000 or comments >= 25:
        return POST_POLL_NEAR_SECONDS
    if views >= 1000 or comments >= 10:
        return POST_POLL_WARM_SECONDS
    return POST_POLL_COLD_SECONDS


def get_managed_pages():
    now_mono = time.monotonic()
    cached = _managed_pages_cache.get("pages") or []
    if cached and now_mono - float(_managed_pages_cache.get("at", 0)) < MANAGED_PAGES_CACHE_SECONDS:
        return cached

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
            detail = _fb_error_detail(data=data)
            print(f"[LỖI] Không lấy được danh sách Page: {_mask_secrets(detail)}")
            send_error_alert(
                "get_managed_pages",
                f"Không lấy được danh sách Page từ USER_ACCESS_TOKEN.\n{detail}",
            )
            return []
        pages.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")
        url = next_url
        params = None

    # Khử trùng Page ID nếu Facebook trả lặp qua pagination.
    unique = {}
    for page in pages:
        pid = str(page.get("id") or "")
        if pid:
            unique[pid] = page
    pages = list(unique.values())

    if INCLUDE_PAGE_NAMES:
        pages = [p for p in pages if p.get("name") in INCLUDE_PAGE_NAMES]
    resolve_error_alert("get_managed_pages", "Da lay duoc danh sach Page Facebook.")
    _managed_pages_cache["pages"] = pages
    _managed_pages_cache["at"] = time.monotonic()

    print(f"[FACEBOOK] Tổng Page từ USER_ACCESS_TOKEN: {len(pages)}")
    for p in pages:
        print(f"[PAGE] {p.get('name') or '(không tên)'} | ID={p.get('id')}")
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
        except Exception as e:
            note_fb_network_failure(f"posts_{page_id}", f"Page {_page_label(page_id)}: không kết nối được Facebook khi lấy danh sách bài: {type(e).__name__}: {e}")
            return found  # các trang trước vẫn được kiểm tra; trang thiếu sẽ thử lại vòng sau
        try:
            data = r.json()
        except ValueError:
            data = None
        if r.status_code != 200 or not isinstance(data, dict) or "error" in data:
            note_fb_network_failure(f"posts_{page_id}", f"Page {_page_label(page_id)}: không lấy được danh sách bài.\n{_fb_error_detail(r, data)}")
            return found
        note_fb_network_success(f"posts_{page_id}", f"Page {_page_label(page_id)} đã đọc được danh sách bài bình thường.")
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
                found.append(pid)
                seen.add(pid)
        url = data.get("paging", {}).get("next")
        params = None
        if not url or (oldest is not None and oldest < cutoff):
            break
    else:
        print(f"[CẢNH BÁO] Page {page_id}: đã quét {POSTS_MAX_PAGES} trang, có thể còn bài cũ hơn.")
    print(f"[FACEBOOK] Page {page_id}: tìm thấy {len(found)} bài trong {ONLY_POSTS_NEWER_THAN_HOURS} giờ.")
    return found


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
                    "relative_url": f"{pid}?fields=message,comments.limit(0).summary(true),permalink_url",
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
            err = _fb_error_detail(resp, batch_resp if isinstance(batch_resp, dict) else None)
            print(f"[LỖI] Batch request thất bại: {_mask_secrets(err)}")
            send_error_alert("batch_request_error", f"Lấy số liệu bài (Batch API) thất bại.\n{err}")
            continue

        resolve_error_alert("batch_request_error", "Facebook Batch API da tra du lieu hop le.")
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



def get_comment_count_fallback(post_id: str, page_token: str, summary_count=None):
    """Đếm comment bằng pagination khi summary.total_count có thể thấp hơn UI.

    Trả về (effective_count, details).
    - effective_count luôn >= summary_count nếu summary_count hợp lệ.
    - Dùng filter=stream và limit=100 để đếm các comment Graph thực sự trả về.
    - Nếu API lỗi giữa chừng, vẫn giữ summary_count và không làm mất dữ liệu.
    - Chỉ đếm top-level objects Graph trả về; nếu từng comment có comments.summary,
      cộng thêm replies_count để gần hơn với tổng comment/conversation trên UI.
    """
    summary = int(summary_count or 0)
    url = f"{GRAPH_URL}/{post_id}/comments"
    params = {
        "filter": "stream",
        "limit": 100,
        "fields": "id,comments.limit(0).summary(true)",
        "access_token": page_token,
    }

    top_level = 0
    replies = 0
    pages = 0
    complete = True

    while url and pages < COMMENT_COUNT_MAX_PAGES:
        try:
            r = api_get(url, params=params)
            data = r.json()
        except Exception as exc:
            print(f"[COMMENT-COUNT] {post_id}: lỗi đọc comments page {pages + 1}: {exc}")
            complete = False
            break

        if "error" in data or not isinstance(data.get("data"), list):
            err = data.get("error", {}).get("message", "response không hợp lệ")
            print(f"[COMMENT-COUNT] {post_id}: Graph API lỗi: {err}")
            complete = False
            break

        rows = data.get("data", [])
        top_level += len(rows)

        for row in rows:
            try:
                replies += int(
                    ((row.get("comments") or {}).get("summary") or {}).get("total_count") or 0
                )
            except (TypeError, ValueError):
                pass

        url = data.get("paging", {}).get("next")
        params = None
        pages += 1

    if url:
        complete = False

    enumerated = top_level + replies

    # Facebook UI của Page này khớp sát nhất với số comment top-level.
    # Vì vậy khi pagination đọc HOÀN TẤT, dùng top_level để xét threshold,
    # KHÔNG cộng replies để tránh đếm trùng (ví dụ 73 + 34 = 107).
    #
    # Nếu pagination không hoàn tất/lỗi, không được dùng số đếm dở dang;
    # fallback về summary để tránh quyết định từ dữ liệu thiếu.
    effective = top_level if complete else summary

    print(
        f"[COMMENT-COUNT] {post_id}: summary={summary} | top_level={top_level} "
        f"| replies={replies} | enumerated={enumerated} | effective(top_level)={effective} "
        f"| pages={pages} | complete={complete}"
    )
    return effective, {
        "summary": summary,
        "top_level": top_level,
        "replies": replies,
        "enumerated": enumerated,
        "pages": pages,
        "complete": complete,
    }


def should_deep_count_comments(views: int, comments: int) -> bool:
    """Deep-count mọi bài có khả năng đạt/gần ngưỡng để Telegram dùng TOP_LEVEL sát Facebook."""
    views = int(views or 0)
    comments = int(comments or 0)

    # Nếu views đã đạt bất kỳ rule nào, cần top_level chính xác trước khi xét/báo.
    for rule in THRESHOLD_RULES:
        if views >= int(rule["min_views"]):
            return True

    # Hoặc summary comments đang gần mốc comments-only.
    if comments >= max(0, COMMENT_ONLY_THRESHOLD - COMMENT_COUNT_NEAR_THRESHOLD):
        return True

    # Hoặc summary đang gần min_comments của bất kỳ rule.
    for rule in THRESHOLD_RULES:
        min_comments = int(rule["min_comments"])
        if min_comments > 0 and comments >= max(0, min_comments - COMMENT_COUNT_NEAR_THRESHOLD):
            return True

    return False

def contains_article_link(text: str) -> bool:
    """Nhận diện link bài đọc cần tránh tạo TXT. Domain cấu hình riêng để không nhầm URL Facebook/permalink."""
    value = (text or "").lower()
    return any(domain in value for domain in ARTICLE_LINK_DOMAINS)


def post_already_has_link(post_id: str, page_id: str, page_token: str, post_message: str) -> bool:
    """
    Kiểm tra link chắc hơn:
    1) Caption có URL -> coi là đã gắn link (giữ hành vi cũ).
    2) Quét comment có phân trang, tối đa COMMENT_LINK_MAX_PAGES.
    3) Nếu comment chứa domain bài đọc cấu hình -> coi là đã gắn link, không phụ thuộc from.id.
    4) Nếu Graph API trả đúng from.id của Page và comment có bất kỳ URL -> cũng coi là đã gắn link.
    """
    if URL_PATTERN.search(post_message or ""):
        return True

    url = f"{GRAPH_URL}/{post_id}/comments"
    params = {
        "filter": "stream",
        "limit": 100,
        "fields": "message,from",
        "access_token": page_token,
    }

    pages_checked = 0
    while url and pages_checked < COMMENT_LINK_MAX_PAGES:
        try:
            r = api_get(url, params=params)
            data = r.json()
        except Exception as e:
            print(f"[CẢNH BÁO] {post_id}: không kiểm tra được comment link: {e}")
            return None  # Không xác minh được link: tuyệt đối không coi là chưa có link.

        if "error" in data:
            err = data.get("error", {}).get("message", "Không rõ")
            print(f"[CẢNH BÁO] {post_id}: Graph API lỗi khi kiểm tra comment link: {err}")
            return None  # Facebook lỗi quyền/API: chặn OpenAI.

        if not isinstance(data.get("data"), list):
            return None
        for c in data.get("data", []):
            msg = c.get("message", "") or ""
            from_id = str((c.get("from") or {}).get("id") or "")

            # Domain bài đọc: không phụ thuộc from.id vì Graph đôi khi không trả author ổn định.
            if contains_article_link(msg):
                print(f"[LINK] {post_id}: phát hiện article link trong comment -> SKIP.")
                return True

            # Fallback: nếu xác định chắc comment là của Page thì bất kỳ URL nào cũng được tính.
            if from_id == str(page_id) and URL_PATTERN.search(msg):
                print(f"[LINK] {post_id}: phát hiện URL trong comment của Page -> SKIP.")
                return True

        url = data.get("paging", {}).get("next")
        params = None
        pages_checked += 1

    # Nếu đã chạm giới hạn phân trang mà còn bình luận chưa đọc, kết quả chưa chắc chắn.
    if url:
        print(f"[LINK] {post_id}: chưa đọc hết comments; bỏ qua để tránh tạo trùng.")
        return None
    return False


# ==================== WEBSITE PUBLISH ====================
# Chỉ tự đăng sau khi gửi đủ ba TXT; áp dụng mọi Page đạt ngưỡng thông thường.

def _story_title(part2):
    for line in part2.splitlines():
        line = line.strip().strip("*# ")
        if line and not re.fullmatch(r"PART\s+2", line, re.I):
            return line[:180]
    return "Story continuation"


def _web_part_title(base_title, n, page_number=None):
    """Có mã Page: "PART 2: Tên truyện15 001". Không có mã Page: "PART 2: Tên truyện 001"."""
    title = base_title.strip()
    title = re.sub(r"(?i)^PART\s*[234](?:\s*\(THE END\))?\s*[:\-–—]?\s*", "", title).strip()
    suffix = f" {AUTHOR_CODE}" if AUTHOR_CODE else ""
    prefix = "PART 4 (THE END): " if n == 4 else f"PART {n}: "
    page = str(int(page_number)) if page_number is not None else ""
    return f"{prefix}{title}{page}{suffix}"


def _story_body(raw, part):
    raw = _strip_program_endings(raw)
    raw = re.sub(r"(?im)^\s*PART\s+%d(?:\s*\(THE END\))?\s*$" % part, "", raw).strip()
    if part == 2:
        lines = raw.splitlines()
        if lines:
            raw = "\n".join(lines[1:]).strip()
    return raw


def _to_html(body, navigation=None):
    paragraphs = re.split(r"\n\s*\n", body.strip())
    def _para(p):
        safe = html.escape(p).replace("\n", "<br>")
        # Chuyển **chữ đậm** kiểu markdown thành thẻ HTML, tránh hiện nguyên dấu ** trên web.
        safe = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe, flags=re.S)
        return "<p>" + safe + "</p>"
    content = "\n".join(_para(p) for p in paragraphs if p.strip())
    if navigation:
        content += '<hr><nav aria-label="Story chapters">' + " | ".join(
            '<a href="%s">%s</a>' % (html.escape(url, quote=True), html.escape(label))
            for label, url in navigation
        ) + '</nav>'
    return content


def _web_login():
    """Đăng nhập bằng Laravel session và CSRF; kiểm tra API /me trước khi ghi."""
    if not WEB_ADMIN_EMAIL or not WEB_ADMIN_PASSWORD:
        raise RuntimeError("Thiếu WEB_ADMIN_EMAIL hoặc WEB_ADMIN_PASSWORD")
    session = requests.Session()
    login_url = WEB_BASE_URL + WEB_LOGIN_PATH
    r = session.get(login_url, timeout=25)
    r.raise_for_status()
    patterns = (
        r'<input[^>]*name=["\']_token["\'][^>]*value=["\']([^"\']+)',
        r'<meta[^>]*name=["\']csrf-token["\'][^>]*content=["\']([^"\']+)',
    )
    token = next((html.unescape(m.group(1)) for pattern in patterns if (m := re.search(pattern, r.text, re.I))), None)
    if not token:
        raise RuntimeError("Không tìm thấy CSRF trong form đăng nhập")
    form = {"email": WEB_ADMIN_EMAIL, "password": WEB_ADMIN_PASSWORD, "_token": token}
    result = session.post(login_url, data=form, headers={"Referer": login_url}, timeout=25, allow_redirects=False)
    if result.status_code not in (302, 303):
        raise RuntimeError(f"Đăng nhập không trả redirect hợp lệ: HTTP {result.status_code}")
    redirect = result.headers.get("Location", "")
    if "dashboard" not in redirect and "admin" not in redirect:
        raise RuntimeError("Đăng nhập không chuyển đến Admin; có thể sai thông tin")
    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    # Laravel session thường xoay CSRF token sau login: lấy token mới ở trang admin.
    dashboard = session.get(WEB_BASE_URL + "/admin/dashboard", timeout=25)
    dashboard.raise_for_status()
    fresh = re.search(r'<meta[^>]*name=["\']csrf-token["\'][^>]*content=["\']([^"\']+)', dashboard.text, re.I)
    if fresh:
        token = html.unescape(fresh.group(1))
    session.headers["X-CSRF-TOKEN"] = token
    # Cookie XSRF-TOKEN tự động được requests gửi kèm.
    xsrf = session.cookies.get("XSRF-TOKEN")
    if xsrf:
        from urllib.parse import unquote
        session.headers["X-XSRF-TOKEN"] = unquote(xsrf)
    me = session.get(WEB_BASE_URL + "/admin/api/v1/me", timeout=25)
    if me.status_code != 200:
        raise RuntimeError(f"Không xác minh được phiên Admin qua /me: HTTP {me.status_code}")
    try:
        info = me.json()
    except ValueError:
        raise RuntimeError("API /me không trả JSON")
    if not info:
        raise RuntimeError("API /me trả dữ liệu rỗng")
    return session


def _facebook_original_image(post_id, page_token):
    """Tìm ảnh gốc của post, không sử dụng ảnh thumbnail nếu có bản full."""
    obj = None
    for fields in ("full_picture,attachments{type,media,subattachments{media,type}}", "full_picture"):
        try:
            response = requests.get(f"{GRAPH_URL}/{post_id}", params={
                "fields": fields, "access_token": page_token}, timeout=30)
            data = response.json() if response.content else {}
        except (requests.RequestException, ValueError) as exc:
            print(f"[FB-IMAGE] {post_id}: lỗi mạng khi lấy ảnh ({type(exc).__name__})", flush=True)
            continue
        if response.status_code == 200 and isinstance(data, dict) and not data.get("error"):
            obj = data
            break
        err = (data.get("error") or {}).get("message", "") if isinstance(data, dict) else ""
        print(f"[FB-IMAGE] {post_id}: HTTP {response.status_code} với fields={fields.split(',')[0]}...: {err[:200]}", flush=True)
    if obj is None:
        return None  # Không lấy được ảnh: bài vẫn đăng (feature_image không bắt buộc theo tài liệu API)
    attachments = ((obj.get("attachments") or {}).get("data") or [])
    candidates = []
    for att in attachments:
        candidates.append(((att.get("media") or {}).get("image") or {}).get("src"))
        for sub in ((att.get("subattachments") or {}).get("data") or []):
            candidates.append(((sub.get("media") or {}).get("image") or {}).get("src"))
    candidates.append(obj.get("full_picture"))
    return next((u for u in candidates if isinstance(u, str) and u.startswith("https://")), None)


def _download_facebook_image(image_url):
    """Giới hạn domain và dung lượng, tránh truy cập URL tùy ý."""
    host = (urlparse(image_url).hostname or "").lower()
    if not (host == "fbcdn.net" or host.endswith(".fbcdn.net") or
            host == "fbsbx.com" or host.endswith(".fbsbx.com")):
        raise RuntimeError("URL ảnh Facebook không thuộc miền CDN được phép")
    with requests.get(image_url, timeout=50, stream=True, allow_redirects=False) as response:
        response.raise_for_status()
        chunks, size = [], 0
        for chunk in response.iter_content(128 * 1024):
            size += len(chunk)
            if size > WEB_IMAGE_MAX_BYTES:
                raise RuntimeError("Ảnh Facebook vượt giới hạn 15MB")
            chunks.append(chunk)
        raw = b"".join(chunks)
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return raw, "image/png", "png"
    if raw.startswith(b"\xff\xd8\xff"):
        return raw, "image/jpeg", "jpg"
    if raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        return raw, "image/webp", "webp"
    raise RuntimeError("Không nhận diện được ảnh PNG/JPEG/WebP từ Facebook")


def _web_upload_featured_image(session, post_id, page_token):
    image_url = _facebook_original_image(post_id, page_token)
    if not image_url:
        if WEB_ALLOW_IMAGE_FALLBACK and WEB_POST_IMAGE_URL.startswith("https://"):
            return WEB_POST_IMAGE_URL
        raise RuntimeError("Không lấy được ảnh gốc Facebook; dừng đăng web")
    raw, content_type, ext = _download_facebook_image(image_url)
    # Web lỗi 5xx khi xin URL upload: thử lại với tên file khác.
    names = [f"fb_{post_id.replace('_', '-')}.{ext}", f"fb-{int(time.time())}.{ext}", f"img-{int(time.time())}.{ext}"]
    resp = None
    for attempt, fname in enumerate(names, 1):
        payload = {"fileName": fname, "contentType": content_type, "size": len(raw),
                   "auditContext": {"record_type": "Post", "action_label": "upload_featured_image"}}
        resp = session.post(WEB_BASE_URL + "/admin/api/v1/uploads/presigned-image-url",
            json=payload, headers={"Origin": WEB_BASE_URL,
                                   "Referer": WEB_BASE_URL + "/admin/posts/new"}, timeout=40)
        if resp.status_code < 500:
            break
        print(f"[WEB-IMAGE] presigned HTTP {resp.status_code} (lần {attempt}/{len(names)}): {resp.text[:160]!r}", flush=True)
        if attempt < len(names):
            time.sleep(4 * attempt)
    resp.raise_for_status()
    data = (resp.json().get("data") or {})
    upload = data.get("upload") or {}
    signed_url, public_url = upload.get("url"), data.get("fileUrl")
    if upload.get("method") != "PUT" or not signed_url or not public_url:
        raise RuntimeError("API cấp URL upload thiếu upload.url/fileUrl")
    signed_host = (urlparse(signed_url).hostname or "").lower()
    public_host = (urlparse(public_url).hostname or "").lower()
    # Mỗi website có thể dùng kho ảnh khác nhau, nên không gắn cứng domain; chỉ bắt buộc HTTPS.
    if not (signed_host and public_host and
            urlparse(signed_url).scheme == urlparse(public_url).scheme == "https"):
        raise RuntimeError("API trả URL ảnh không hợp lệ (thiếu domain hoặc không phải HTTPS)")
    headers = dict(upload.get("headers") or {})
    headers["Content-Type"] = content_type
    # requests tự xác định Content-Length khớp bytes; không chuyển Cookie admin lên R2.
    headers.pop("Content-Length", None)
    put = requests.put(signed_url, data=raw, headers=headers, timeout=100)
    if put.status_code not in (200, 201, 204):
        raise RuntimeError(f"R2 upload ảnh thất bại HTTP {put.status_code}")
    return public_url


def _web_featured_image_or_fallback(post_id, page_token, session=None):
    """Tải ảnh lên web; lỗi tạm thời (500/522...) thì thử lại (đăng nhập lại); nếu vẫn lỗi thì dùng
    thẳng link ảnh gốc Facebook để KHÔNG chặn việc đăng bài. Trả (url, dùng_fallback)."""
    last = None
    for attempt in range(1, 4):
        try:
            s = session if (attempt == 1 and session is not None) else _web_login()
            return _web_upload_featured_image(s, post_id, page_token), "web"
        except Exception as exc:
            last = exc
            print(f"[WEB-IMAGE] tải ảnh lên web lỗi (lần {attempt}/3): {exc}", flush=True)
            if attempt < 3:
                time.sleep(6 * attempt)
    fb_url = _facebook_original_image(post_id, page_token)
    if not fb_url:
        print(f"[WEB-IMAGE] {post_id}: không có ảnh nào dùng được; đăng bài KHÔNG ảnh đại diện", flush=True)
        return "", "none"
    print(f"[WEB-IMAGE] dùng tạm link ảnh Facebook vì web lỗi: {last}", flush=True)
    return fb_url, "fb"


def _web_create_post(session, title, body_html, image_url, slug=""):
    payload = {
        "title": title, "slug": slug, "description": body_html,
        "image": image_url, "is_active": WEB_POSTS_PUBLIC,
        "is_home": False, "is_top": False, "next_chapter": "", "prev_chapter": "",
        "selected_categories": [], "selected_tags": [],
        "seo_description": "", "seo_keywords": "", "seo_title": "", "series_id": None,
    }
    response = session.post(WEB_BASE_URL + "/admin/api/v1/posts", json=payload, timeout=45,
                            headers={"Origin": WEB_BASE_URL, "Referer": WEB_BASE_URL + "/admin/posts/new"})
    if response.status_code != 201:
        raise RuntimeError(f"Website HTTP {response.status_code}: {response.text[:250]}")
    data = response.json()
    if data.get("ok") is not True:
        raise RuntimeError("Website không xác nhận ok=true")
    record = data.get("data") or {}
    link = record.get("public_url") or record.get("permalink")
    parsed = urlparse(link or "")
    if parsed.scheme != "https" or parsed.netloc != urlparse(WEB_BASE_URL).netloc:
        raise RuntimeError("Website trả về public_url không hợp lệ")
    return {"url": link, "id": record.get("id"), "slug": record.get("slug")}


def _web_update_chapter(session, post_record, title, description, image_url, prev_slug="", next_slug=""):
    """PUT đúng payload đã quan sát trong DevTools, với slug chương liền kề."""
    post_id = post_record.get("id")
    if not post_id or not post_record.get("slug"):
        raise RuntimeError("Thiếu id/slug của bài web")
    payload = {"title": title, "slug": post_record["slug"], "description": description,
               "is_active": WEB_POSTS_PUBLIC, "is_home": False, "is_top": False,
               "image": image_url, "selected_categories": [], "selected_tags": [],
               "seo_description": None, "seo_keywords": None, "seo_title": None, "series_id": None,
               "next_chapter": next_slug, "prev_chapter": prev_slug, "version": "1"}
    response = session.put(f"{WEB_BASE_URL}/admin/api/v1/posts/{post_id}", json=payload,
        headers={"Origin": WEB_BASE_URL, "Referer": f"{WEB_BASE_URL}/admin/posts/{post_id}/edit"}, timeout=45)
    if response.status_code != 200:
        print(f"[WEB-NAV-ERROR] PUT post_id={post_id} HTTP={response.status_code} body={response.text[:400]}", flush=True)
        raise RuntimeError(f"Không cập nhật được liên kết chương post_id={post_id}: HTTP {response.status_code}: {response.text[:200]}")
    result = response.json()
    if result.get("ok") is not True:
        print(f"[WEB-NAV-ERROR] PUT post_id={post_id} JSON={str(result)[:400]}", flush=True)
        raise RuntimeError(f"API sửa bài post_id={post_id} không xác nhận ok=true")


def _wp_request(method, path, **kwargs):
    if not WP_USERNAME or not WP_APPLICATION_PASSWORD:
        raise RuntimeError("Thiếu WP_USERNAME hoặc WP_APPLICATION_PASSWORD")
    response = requests.request(method, WP_BASE_URL + path,
        auth=(WP_USERNAME, WP_APPLICATION_PASSWORD), timeout=65, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"WordPress {method} {path}: HTTP {response.status_code}: {response.text[:350]}")
    return response.json()


def _wp_upload_featured_image(post_id, page_token):
    source = _facebook_original_image(post_id, page_token)
    if not source:
        raise RuntimeError("Không có ảnh Facebook gốc để đăng WordPress")
    raw, mime, ext = _download_facebook_image(source)
    filename = f"fb_{post_id.replace('_', '-')}.{ext}"
    response = _wp_request("POST", "/wp-json/wp/v2/media", data=raw,
        headers={"Content-Type": mime, "Content-Disposition": f'attachment; filename="{filename}"'})
    if not response.get("id"):
        raise RuntimeError("WordPress không trả media ID")
    return int(response["id"])


def _wp_create_post(title, body_html, media_id):
    record = _wp_request("POST", "/wp-json/wp/v2/posts", json={
        "title": title, "content": body_html,
        "status": "publish" if WP_POSTS_PUBLIC else "draft", "featured_media": media_id})
    if not record.get("id"):
        raise RuntimeError("WordPress không trả post ID")
    return {"id": record["id"], "url": record.get("link", ""), "slug": record.get("slug", "")}


def _wp_update_post(post_record, body_html):
    return _wp_request("POST", f"/wp-json/wp/v2/posts/{int(post_record['id'])}", json={"content": body_html})


# WordPress: ending khớp nguyên văn TXT, các liên kết chương là thẻ <a> bấm được.
def _wp_chapter_html(parts, n, records):
    content = _to_html(_story_body(parts[str(n)], n))
    content += "\n" + _to_html(PART_ENDINGS[n])
    links = []
    # Chỉ có nút NEXT từ Part 3 sang Part 4; Part 2 và Part 4 không có điều hướng.
    if n == 3:
        url = records.get("4", {}).get("url")
        if not url:
            raise RuntimeError("Thiếu URL Part 4 để tạo liên kết NEXT từ Part 3")
        links.append(("NEXT: PART 4(THE END)", url))
    if links:
        content += '\n<nav aria-label="Story chapter links" class="story-chapter-links">'
        for label, url in links:
            parsed = urlparse(url)
            if parsed.scheme != "https" or parsed.netloc != urlparse(WP_BASE_URL).netloc:
                raise RuntimeError("URL chương WordPress không thuộc website đang chọn")
            content += '<p><a href="%s"><strong>%s</strong></a></p>' % (
                html.escape(url, quote=True), html.escape(label))
        content += '</nav>'
    return content


def repair_existing_wordpress_links(post_id):
    """Chỉ cập nhật 3 bài WordPress đã tồn tại; KHÔNG tạo bài, ảnh hay gọi OpenAI."""
    state = _load_progress(post_id)
    if state.get("publish_platform") != "wordpress" or state.get("publish_target") != WP_BASE_URL:
        raise RuntimeError("Checkpoint không thuộc WordPress đang chọn")
    records = state.get("web_posts") or {}
    parts = state.get("parts") or {}
    if not all(records.get(str(n), {}).get("id") and records[str(n)].get("url")
               and parts.get(str(n)) for n in (2, 3, 4)):
        raise RuntimeError("Checkpoint chưa đủ 3 chương WordPress; không sửa để tránh sai bài")
    for n in (2, 3, 4):
        _wp_update_post(records[str(n)], _wp_chapter_html(parts, n, records))
        print(f"[WP-REPAIR] {post_id}: updated Part {n}")
    state["wp_chapter_format_version"] = 4
    _save_progress(post_id, state)


def _wp_repair_requested_once():
    post_id = os.getenv("WP_REPAIR_EXISTING_POST_ID", "").strip()
    if not post_id:
        return
    if not re.fullmatch(r"\d+_\d+", post_id):
        print("[WP-REPAIR] WP_REPAIR_EXISTING_POST_ID không hợp lệ")
        return
    try:
        repair_existing_wordpress_links(post_id)
    except Exception as exc:
        print(f"[WP-REPAIR] {post_id}: lỗi: {exc}")


def publish_wordpress_story(post_id):
    post_id = str(post_id).strip()
    if not re.fullmatch(r"\d+_\d+", post_id):
        raise ValueError("POST_ID không hợp lệ")
    state = _load_progress(post_id)
    if not all(str(n) in (state.get("parts") or {}) for n in (2, 3, 4)) or not all(n in state.get("sent", []) for n in (2, 3, 4)):
        raise RuntimeError("Chưa gửi đủ ba TXT")
    if not ENABLE_WEB_PUBLISH_TEST:
        raise RuntimeError("ENABLE_WEB_PUBLISH_TEST=false")
    target = WP_BASE_URL
    if state.get("publish_target") and state["publish_target"] != target:
        raise RuntimeError("Checkpoint thuộc website khác; không tự chuyển bài đã đăng dở")
    if state.get("web_posts") and not state.get("publish_target"):
        raise RuntimeError("Checkpoint cũ đã có bài web nhưng chưa ghi domain; cần xác minh thủ công")
    state["publish_target"] = target
    state["publish_platform"] = "wordpress"
    _save_progress(post_id, state)
    if state.get("web_posts") and not state.get("publish_target"):
        raise RuntimeError("Checkpoint cũ đã có bài web nhưng chưa ghi domain; cần xác minh thủ công")
    records = state.setdefault("web_posts", {})
    parts = state["parts"]
    base_title = _story_title(parts["2"])
    page_id = post_id.split("_", 1)[0]
    page_name = state.get("page_name")
    if not page_name:
        page = next((p for p in get_managed_pages() if str(p.get("id")) == page_id), None)
        page_name = (page or {}).get("name")
    media_id = state.get("wp_media_id")
    if not media_id:
        page = next((p for p in get_managed_pages() if str(p.get("id")) == page_id), None)
        if not page or not page.get("access_token"):
            raise RuntimeError("Thiếu Facebook Page token để lấy ảnh")
        media_id = _wp_upload_featured_image(post_id, page["access_token"])
        state["wp_media_id"] = media_id
        _save_progress(post_id, state)
    for n in (4, 3, 2):
        key = str(n)
        if records.get(key, {}).get("id"):
            continue
        nav = [(f"READ PART {n+1}", records[str(n+1)]["url"])] if n < 4 and records.get(str(n+1), {}).get("url") else []
        title = _web_part_title(base_title, n)
        records[key] = _wp_create_post(title, _to_html(_story_body(parts[key], n)) + "\n" + _to_html(PART_ENDINGS[n]), media_id)
        _save_progress(post_id, state)
        print(f"[WORDPRESS] {post_id}: created Part {n}, ID={records[key]['id']}")
    # Version 3: đồng bộ nhãn NEXT: PART 4(THE END) kể cả checkpoint cũ.
    if state.get("wp_chapter_format_version") != 4:
        for n in (2, 3, 4):
            _wp_update_post(records[str(n)], _wp_chapter_html(parts, n, records))
            _save_progress(post_id, state)
        state["wp_chapter_format_version"] = 4
        state["web_linked"] = [2, 3, 4]
        _save_progress(post_id, state)
    if not state.get("web_links_telegram_sent"):
        message = "\n".join(f"PART {n}: {records[str(n)]['url']}" for n in (2, 3, 4))
        if not all(records[str(n)].get("url") for n in (2, 3, 4)):
            raise RuntimeError("WordPress thiếu link chương")
        if not send_telegram_message(message):
            raise RuntimeError("Telegram chưa xác nhận thông báo ba link")
        state["web_links_telegram_sent"] = True
        _save_progress(post_id, state)


def _web_publish_allowed(post_id, state=None):
    """WEB_PUBLISH_PAGES=all hoặc danh sách Page ID / tên Page."""
    if WEB_PUBLISH_ALL_PAGES:
        return True
    state = state if state is not None else _load_progress(post_id)
    page_id = str(post_id).split("_", 1)[0].casefold()
    page_name = re.sub(r"\s+", " ", str(state.get("page_name") or "")).strip().casefold()
    return page_id in WEB_PUBLISH_PAGE_KEYS or (page_name and page_name in WEB_PUBLISH_PAGE_KEYS)


def _bioblog_permalink(title):
    """Slug lấy từ tiêu đề (đã chứa AUTHOR_CODE); server tự ghép author vào cuối slug."""
    value = unicodedata.normalize("NFKD", title)
    value = "".join(c for c in value if not unicodedata.combining(c))
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    # Server tự ghép "-<username>" vào cuối; nếu username trùng mã nhân viên thì bỏ để link không lặp "-001-001".
    author = re.sub(r"[^a-z0-9]+", "-", AUTHOR_USERNAME.lower()).strip("-")
    if author and slug.endswith("-" + author):
        slug = slug[: -len(author) - 1]
    return slug


class BioBlogHTTPError(RuntimeError):
    def __init__(self, status, text):
        super().__init__(f"BioBlog HTTP {status}: {text[:350]}")
        self.status = status


def _bioblog_create(title, content, image_url, permalink, prev_slug="", next_chapter=""):
    """Tạo 1 bài qua API công khai. Không retry POST vì request có thể đã tạo bài dù mất phản hồi."""
    if image_url and not image_url.startswith("https://"):
        raise RuntimeError("Ảnh đại diện phải là link HTTPS")
    payload = {"title": title, "content": content, "author": AUTHOR_USERNAME,
               "permalink": permalink, "mode": "normal"}
    if image_url:
        payload["feature_image"] = image_url  # không bắt buộc theo tài liệu API
    if prev_slug:
        payload["prev_chapter"] = prev_slug  # server tự gắn next_chapter của bài trước trỏ lại
    if next_chapter:
        payload["next_chapter"] = next_chapter
    resp = requests.post(WEB_BASE_URL + "/api/posts",
        headers={"Authorization": "Bearer " + BIOBLOG_API_TOKEN, "Accept": "application/json"},
        json=payload, timeout=65)
    if resp.status_code not in (200, 201):
        raise BioBlogHTTPError(resp.status_code, resp.text)
    obj = resp.json()
    data = obj.get("data", obj) if isinstance(obj, dict) else {}
    if not isinstance(data, dict):
        raise RuntimeError("BioBlog trả JSON không đúng cấu trúc")
    url = data.get("link") or data.get("frontend_link") or data.get("url") or data.get("public_url")
    slug = data.get("slug")
    if not url and slug:
        url = WEB_BASE_URL + "/blog/" + str(slug).lstrip("/")
    if not url or urlparse(str(url)).netloc != urlparse(WEB_BASE_URL).netloc:
        raise RuntimeError("BioBlog không trả link thuộc domain đã cấu hình; cần kiểm tra thủ công")
    print(f"[SLUG] permalink gửi={permalink!r}, slug server trả={slug!r}", flush=True)
    return {"id": data.get("postId") or data.get("post_id") or data.get("id"), "slug": slug, "url": str(url),
            "author_sent": AUTHOR_USERNAME, "permalink_sent": permalink}


def _bioblog_check_author(post_id, rec):
    """Chỉ đọc: cảnh báo nếu bài không thuộc đúng tài khoản AUTHOR_USERNAME."""
    try:
        resp = requests.get(WEB_BASE_URL + "/api/posts/" + str(rec["slug"]).lstrip("/"),
            headers={"Authorization": "Bearer " + BIOBLOG_API_TOKEN, "Accept": "application/json"}, timeout=40)
        obj = resp.json() if resp.status_code == 200 else {}
        data = obj.get("data", obj) if isinstance(obj, dict) else {}
        if isinstance(data, dict) and isinstance(data.get("post"), dict):
            data = data["post"]
        author = data.get("author") if isinstance(data, dict) else None
        print(f"[AUTHOR-CHECK] {post_id}: gửi author={AUTHOR_USERNAME!r}, server ghi author={author!r}", flush=True)
        uname = author.get("username") if isinstance(author, dict) else (author if isinstance(author, str) else None)
        if uname and str(uname).strip().lstrip("@").lower() != AUTHOR_USERNAME.lower():
            send_error_alert("author_mismatch",
                f"Bài đăng lên web đang thuộc tài khoản {uname!r}, KHÔNG phải {AUTHOR_USERNAME!r}. "
                f"Kiểm tra biến AUTHOR_USERNAME phải đúng cột Username của tài khoản bạn trên web.")
    except Exception as exc:
        print(f"[AUTHOR-CHECK] {post_id}: không kiểm tra được: {exc}", flush=True)


STORY_GROUP_MAX_GAP_SECONDS = 180


def _resend_story_group(post_id, state):
    """Tin link đến trễ (đăng bù bài cũ / web lỗi rồi thử lại): gửi lại đầu tin + 3 TXT ngay trước tin link,
    để trên Telegram luôn thấy đủ bộ: tin báo -> 3 TXT -> 3 link. Không gọi OpenAI (dùng Part đã lưu)."""
    page_name = state.get("page_name") or post_id.split("_", 1)[0]
    parts = state["parts"]
    header = (f"🔁 BÀI ĐÃ CÓ TRUYỆN — GỬI LẠI ĐỦ BỘ (Page: {page_name})\n"
              f"Link: https://www.facebook.com/{post_id}\n"
              "=> Gắn link ngay!")
    if send_telegram_message(header) is False:
        raise RuntimeError("Telegram chưa xác nhận tin đầu nhóm")
    story = "\n\n".join((
        ("=" * 60 + "\n\n" if n > 2 else "") + parts[str(n)] + "\n\n**" + PART_ENDINGS[n] + "**"
        for n in (2, 3, 4)
    ))
    outputs = split_story_into_parts(story)
    for n in (2, 3, 4):
        path = f"/tmp/story_{re.sub(r'[^a-zA-Z0-9_]', '_', str(post_id))}_PART_{n}.txt"
        try:
            create_part_txt(outputs[n], n, path)
            if not send_telegram_document(path, caption=f"📄 PART {n} — Page: {page_name}"):
                raise RuntimeError(f"Gửi lại TXT Part {n} thất bại")
        finally:
            if os.path.exists(path):
                os.remove(path)
    state["txt_group_at"] = time.time()
    _save_progress(post_id, state)
    print(f"[GROUP-RESEND] {post_id}: đã gửi lại đầu tin + 3 TXT trước tin link", flush=True)


def publish_bioblog_api(post_id):
    """Đăng Part 2 -> 3 -> 4 qua /api/posts, nối chương bằng prev_chapter, gửi 1 tin Telegram đủ 3 link."""
    post_id = str(post_id).strip()
    if not re.fullmatch(r"\d+_\d+", post_id):
        raise ValueError("POST_ID phải có dạng PAGEID_POSTID")
    if not WEB_BASE_URL.startswith("https://"):
        raise RuntimeError("WEB_API_URL phải là HTTPS")
    if not BIOBLOG_API_TOKEN:
        raise RuntimeError(f"Thiếu token API cho {_ACTIVE_WEB_HOST}; thêm vào WEB_SITES")
    state = _load_progress(post_id)
    parts = state.get("parts") or {}
    if not all(str(n) in parts for n in (2, 3, 4)) or not all(n in state.get("sent", []) for n in (2, 3, 4)):
        raise RuntimeError("Chưa đủ 3 TXT gửi Telegram; không xuất bản")
    if state.get("publish_target") and state["publish_target"] != WEB_BASE_URL:
        raise RuntimeError("Checkpoint thuộc website khác; không tự chuyển bài đã đăng dở")
    state["publish_target"] = WEB_BASE_URL
    state["publish_platform"] = "bioblog_api"
    posts = state.setdefault("web_posts", {})
    _save_progress(post_id, state)

    base_title = _story_title(parts["2"])
    page_number = _page_number_or_none(state.get("page_name"))
    titles = {n: _web_part_title(base_title, n, page_number) for n in (2, 3, 4)}
    slugs = {n: _bioblog_permalink(titles[n]) for n in (2, 3, 4)}
    if len(set(slugs.values())) != 3:
        raise RuntimeError("Trùng permalink giữa các Part")

    # Ảnh đại diện dùng chung 3 Part.
    image_url = state.get("web_featured_image") or ""
    if "web_featured_image" not in state:
        if any((posts.get(str(n)) or {}).get("request_started") or (posts.get(str(n)) or {}).get("url") for n in (2, 3, 4)):
            raise RuntimeError("Checkpoint đã có bài nhưng thiếu ảnh gốc; không tiếp tục để tránh sai ảnh")
        page_id = post_id.split("_", 1)[0]
        page = next((p for p in get_managed_pages() if str(p.get("id")) == page_id), None)
        if not page or not page.get("access_token"):
            raise RuntimeError("Không có Facebook Page token để lấy ảnh gốc")
        image_url, image_mode = _web_featured_image_or_fallback(post_id, page["access_token"])
        if image_url and not image_url.startswith("https://"):
            raise RuntimeError("Ảnh upload không có HTTPS URL")
        state["web_featured_image"] = image_url
        if image_mode in ("fb", "none"):
            state["web_image_fallback_pending"] = image_mode  # báo SAU tin 3 link để giữ đúng thứ tự
        _save_progress(post_id, state)
        print(f"[AUTO-WEB] {post_id}: đã có ảnh đại diện dùng chung 3 Part", flush=True)

    # Điều hướng như bản gốc: Part 2 đứng riêng (người đọc quay lại comment lấy link Part 3),
    # chỉ Part 3 có nút NEXT sang Part 4. Tạo Part 4 trước Part 3 để có link Part 4.
    # next_chapter gửi dạng URL đầy đủ: theo tài liệu API, URL không làm web tự gắn nút ngược lại.
    for n in (2, 4, 3):
        key = str(n)
        if (posts.get(key) or {}).get("url"):
            continue
        if (posts.get(key) or {}).get("request_started"):
            raise RuntimeError(f"Part {n} đã gọi POST nhưng chưa rõ kết quả; kiểm tra web thủ công trước khi tiếp tục")
        prev_slug = ""
        next_chapter = ""
        if n == 3:
            next_chapter = (posts.get("4") or {}).get("url") or ""
            if not next_chapter:
                raise RuntimeError("Part 4 chưa có URL để làm nút NEXT của Part 3")
        # Link chữ cuối bài (luật riêng bản này): CHỈ Part 3 có "NEXT: PART 4(THE END)", dùng link thật của Part 4.
        text_nav = [("NEXT: PART 4(THE END)", next_chapter)] if n == 3 else None
        body = _to_html(_story_body(parts[key], n) + "\n\n" + PART_ENDINGS[n], text_nav)
        # Đánh dấu TRƯỚC khi POST: mất phản hồi giữa chừng thì không tự tạo lại (tránh đăng trùng).
        posts[key] = {"request_started": True, "started_at": datetime.now(timezone.utc).isoformat()}
        _save_progress(post_id, state)
        try:
            result = _bioblog_create(titles[n], body, image_url, slugs[n], prev_slug, next_chapter)
        except BioBlogHTTPError as exc:
            # Web trả 4xx = đã từ chối, chắc chắn chưa tạo bài -> gỡ dấu để lần sau thử lại an toàn.
            if 400 <= exc.status < 500 and exc.status not in (408, 409):
                posts.pop(key, None)
                _save_progress(post_id, state)
            raise
        posts[key] = result
        _save_progress(post_id, state)
        print(f"[AUTO-WEB] {post_id}: đã đăng Part {n}: {result.get('url')} slug={result.get('slug')}", flush=True)

    urls = {n: (posts.get(str(n)) or {}).get("url") for n in (2, 3, 4)}
    if not all(urls.values()) or len(set(urls.values())) != 3:
        raise RuntimeError("Thiếu hoặc trùng URL chương; cần kiểm tra thủ công")
    # Chỉ gửi MỘT tin Telegram chứa đủ 3 URL; lưu checkpoint để retry/redeploy không gửi lại.
    # Thứ tự Telegram của mỗi bài: BÀI ĐANG LÊN -> 3 file TXT -> tin 3 link; mọi cảnh báo phụ gửi sau.
    if not state.get("web_links_telegram_sent"):
        if time.time() - float(state.get("txt_group_at") or 0) > STORY_GROUP_MAX_GAP_SECONDS:
            _resend_story_group(post_id, state)
        # Dòng đầu ghi tên Page để biết ngay link thuộc bài nào (kể cả khi tin link đến trễ do web lỗi).
        message = f"🔗 LINK TRUYỆN (Page: {state.get('page_name') or post_id.split('_', 1)[0]})\n" + "\n".join(
            f"PART {n}: {urls[n]}" for n in (2, 3, 4))
        if send_telegram_message(message) is False:
            raise RuntimeError("Telegram chưa xác nhận gửi thông báo 3 link")
        state["web_links_telegram_sent"] = True
        _save_progress(post_id, state)
        print(f"[WEB-LINKS] {post_id}: đã gửi đủ 3 URL", flush=True)
    if state.get("web_image_fallback_pending"):
        warn = ("⚠️ Bài trên web CHƯA có ảnh đại diện (không lấy được ảnh từ Facebook/web). Nên thêm ảnh trong trang quản trị."
                if state["web_image_fallback_pending"] == "none" else
                "⚠️ Web lỗi khi tải ảnh lên (HTTP 500/đăng nhập). Bài vẫn đăng, ảnh đại diện đang dùng tạm link ảnh Facebook — có thể hết hạn, nên thay ảnh trong trang quản trị sau.")
        if send_telegram_message(f"{warn}\n(Page: {state.get('page_name') or post_id})"):
            state.pop("web_image_fallback_pending", None)
            _save_progress(post_id, state)
    _bioblog_check_author(post_id, posts["2"])
    return True


def publish_approved_story(post_id):
    if PUBLISH_PLATFORM == "wordpress":
        return publish_wordpress_story(post_id)
    if not _web_publish_allowed(post_id):
        raise RuntimeError("Page này chưa được bật đăng web (xem biến WEB_PUBLISH_PAGES)")
    return publish_bioblog_api(post_id)



AUTO_PUBLISH_RETRY_SECONDS = int(os.getenv("AUTO_PUBLISH_RETRY_SECONDS", "300"))
_AUTO_PUBLISH_LOCK = threading.RLock()


def _auto_publish_with_retry(post_id):
    """Publish after 3 TXT; retry later from persistent state, never recreate saved chapters."""
    if not (ENABLE_AUTO_WEB_PUBLISH and ENABLE_WEB_PUBLISH_TEST):
        return False
    # Chống treo bot: luôn lấy khóa Telegram TRƯỚC rồi mới lấy khóa đăng web, giống thứ tự của
    # story worker (đang giữ khóa Telegram khi gọi hàm này). Hai luồng lấy khóa cùng thứ tự
    # thì không bao giờ chờ nhau vòng tròn. RLock nên worker lấy lại khóa Telegram vẫn được.
    with TELEGRAM_GROUP_LOCK, _AUTO_PUBLISH_LOCK:
        state = _load_progress(post_id)
        if not state.get("auto_publish_eligible"):
            return False
        if state.get("web_publish_complete"):
            return True
        if PUBLISH_PLATFORM != "wordpress" and not _web_publish_allowed(post_id, state):
            return False
        if state.get("web_publish_blocked"):
            return False  # Đã gửi POST nhưng chưa rõ kết quả: chờ kiểm tra tay, không tự đăng lại.
        if time.time() < float(state.get("web_retry_after", 0)):
            return False
        if not all(str(n) in (state.get("parts") or {}) for n in (2, 3, 4)):
            return False
        if not all(n in state.get("sent", []) for n in (2, 3, 4)):
            return False
        try:
            publish_approved_story(post_id)
            state = _load_progress(post_id)
            state["web_publish_complete"] = True
            state.pop("web_retry_after", None)
            _save_progress(post_id, state)
            resolve_error_alert(f"web_publish_retry_{post_id}", f"Website đã đăng xong bài {post_id}.")
            return True
        except Exception as exc:
            state = _load_progress(post_id)
            web = state.get("web_posts") or {}
            unknown = [k for k, r in web.items() if isinstance(r, dict) and r.get("request_started") and not r.get("url")]
            if unknown:
                # Đã gọi POST nhưng chưa rõ kết quả -> dừng để tránh đăng trùng, cần kiểm tra tay.
                state["web_publish_blocked"] = str(exc)[:500]
                _save_progress(post_id, state)
                print(f"[AUTO-WEB-BLOCKED] {post_id}: Part {unknown} chưa rõ kết quả: {exc}", flush=True)
                send_error_alert(f"web_publish_unknown_{post_id}",
                                 f"Bài {post_id}: đã gửi yêu cầu tạo Part {', '.join(unknown)} nhưng chưa rõ kết quả; "
                                 f"dừng để tránh đăng trùng, kiểm tra web thủ công: {str(exc)[:400]}")
            else:
                # Lỗi chưa tạo bài (web 500, đăng nhập, ảnh...) hoặc lỗi nối chương/gửi link: tự thử lại an toàn.
                state["web_retry_after"] = time.time() + AUTO_PUBLISH_RETRY_SECONDS
                state["web_last_error"] = str(exc)[:800]
                _save_progress(post_id, state)
                print(f"[AUTO-WEB] {post_id}: {exc}; retry in {AUTO_PUBLISH_RETRY_SECONDS}s")
                send_error_alert(f"web_publish_retry_{post_id}",
                                 f"Website chưa đăng xong bài {post_id}: {str(exc)[:500]}\n"
                                 "Bot sẽ tự thử lại; các Part đã đăng được lưu checkpoint, không tạo trùng.")
            return False


def auto_publish_recovery_worker():
    """Only recover explicitly eligible NEW posts; old stories never auto-publish."""
    while True:
        try:
            if ENABLE_AUTO_WEB_PUBLISH and ENABLE_WEB_PUBLISH_TEST and os.path.isdir(STORY_PROGRESS_DIR):
                for filename in os.listdir(STORY_PROGRESS_DIR):
                    if not re.fullmatch(r"[A-Za-z0-9_]+\.json", filename):
                        continue
                    post_id = filename[:-5]
                    state = _load_progress(post_id)
                    if state.get("web_publish_complete"):
                        continue
                    if not (all(n in state.get("sent", []) for n in (2, 3, 4))
                            and all(str(n) in (state.get("parts") or {}) for n in (2, 3, 4))):
                        continue
                    if not state.get("auto_publish_eligible"):
                        # Bài cũ đã gửi đủ 3 TXT nhưng chưa lên web: đăng bù nếu trong WEB_BACKFILL_HOURS giờ.
                        age_h = (time.time() - os.path.getmtime(os.path.join(STORY_PROGRESS_DIR, filename))) / 3600
                        if age_h > WEB_BACKFILL_HOURS or state.get("web_posts"):
                            continue
                        state["auto_publish_eligible"] = True
                        _save_progress(post_id, state)
                        print(f"[WEB-BACKFILL] {post_id}: bài đã có 3 TXT nhưng chưa có link web -> đăng bù", flush=True)
                    _auto_publish_with_retry(post_id)
        except Exception as exc:
            print(f"[AUTO-WEB-RECOVERY] {exc}")
        time.sleep(60)



STORY_QUEUE = queue.Queue()
_story_pending = set()
_story_lock = threading.Lock()

def enqueue_story(page_name, post_id, message, page_id, page_token, alert_text, notified_key):
    with _story_lock:
        if (post_id in _story_pending or post_id in story_completed
                or not _retry_due(post_id) or STORY_QUEUE.qsize() >= 3):
            return False
        _story_pending.add(post_id)
    STORY_QUEUE.put((page_name, post_id, message, page_id, page_token, alert_text, notified_key))
    return True

def story_worker():
    while True:
        page_name, post_id, message, page_id, page_token, alert_text, notified_key = STORY_QUEUE.get()
        try:
            if post_id in story_completed or not _retry_due(post_id):
                continue
            if not ENABLE_PAID_GENERATION:
                print(f"[COST GUARD] {post_id}: chưa bật ENABLE_PAID_GENERATION; bỏ qua hàng đợi.")
                continue
            # Đợi đến lượt gửi cả nhóm; scanner vẫn chạy độc lập mỗi 60 giây.
            with TELEGRAM_GROUP_LOCK:
                link_status = post_already_has_link(post_id, page_id, page_token, message)
                if link_status is not False:
                    print(f"[WORKER] {post_id}: đã có link hoặc không xác minh được; không gọi OpenAI.")
                    continue
                try:
                    verify = api_get(f"{GRAPH_URL}/{post_id}", params={"fields": "created_time", "access_token": page_token}).json()
                    published = datetime.strptime(verify["created_time"], "%Y-%m-%dT%H:%M:%S%z")
                    age = datetime.now(timezone.utc) - published
                    if not (timedelta(0) <= age <= timedelta(hours=ONLY_POSTS_NEWER_THAN_HOURS)):
                        print(f"[COST GUARD] {post_id}: ngoài giới hạn {ONLY_POSTS_NEWER_THAN_HOURS} giờ; bỏ qua.")
                        continue
                except (KeyError, ValueError, TypeError, requests.RequestException) as exc:
                    print(f"[COST GUARD] {post_id}: không xác minh được ngày đăng: {exc}; bỏ qua.")
                    continue
                # Đã xác nhận bài mới theo thời gian Facebook: đánh dấu đủ điều kiện auto-web
                # (chỉ có tác dụng khi bật ENABLE_AUTO_WEB_PUBLISH). Lưu trước khi gọi OpenAI.
                state = _load_progress(post_id)
                if not state.get("auto_publish_eligible"):
                    state["auto_publish_eligible"] = True
                    state["page_name"] = state.get("page_name") or page_name
                    _save_progress(post_id, state)
                if notified_key not in already_notified:
                    print(f"[ALERT-SEND] {post_id}: gửi Telegram BÀI ĐANG LÊN.")
                    if not send_telegram_message(alert_text):
                        raise RuntimeError("Telegram khong xac nhan gui thong bao bai dang len")
                    already_notified.add(notified_key)
                    save_notified(already_notified)
                    print(f"[ALERT-SENT] {post_id}: đã lưu notified sau khi gửi Telegram.")
                else:
                    print(f"[WORKER] {post_id}: đã báo trước đó; không báo lặp.")
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

def check_all_pages():
    # Token expiry được kiểm tra theo ngày để vòng quét nhanh hơn.
    if not hasattr(check_all_pages, "_last_token_check") or time.time() - check_all_pages._last_token_check > 86400:
        check_token_expiry()
        check_all_pages._last_token_check = time.time()

    pages = get_managed_pages()
    ts = datetime.now().strftime("%H:%M:%S")

    if not pages:
        print(f"[{ts}] Không tìm thấy Page nào (kiểm tra lại USER_ACCESS_TOKEN).")
        return

    print(f"[{ts}] Đang quét {len(pages)} Page: {', '.join(p['name'] for p in pages)}")

    changed = False
    now_dt = datetime.now()

    for page in pages:
        page_id = page["id"]
        page_name = page["name"]
        page_token = page["access_token"]

        all_post_ids = get_recent_post_ids(page_id, page_token)

        # Không quét lại bài đã gửi đủ TXT. Adaptive polling: bài gần ngưỡng 1 phút,
        # bài xa ngưỡng 2-5 phút. Vòng scanner vẫn chạy mỗi 60 giây để bắt bài mới nhanh.
        now_mono = time.monotonic()
        post_ids_to_check = [
            pid for pid in all_post_ids
            if str(pid) not in story_completed and now_mono >= _post_next_check.get(str(pid), 0)
        ]

        if not post_ids_to_check:
            continue

        stats = get_stats_batch(post_ids_to_check, page_token)

        for post_id in post_ids_to_check:
            key = f"{page_id}_{post_id}"
            views, comments, link, message = stats.get(post_id, (None, None, None, ""))

            if comments is None or (views is None and comments <= COMMENT_ONLY_THRESHOLD):
                print(f"[{ts}] [{page_name}] {post_id}: không lấy được dữ liệu, bỏ qua.")
                # Dữ liệu lỗi: thử lại ngay vòng 60 giây kế tiếp, không cache kết quả lỗi lâu.
                _post_next_check[str(post_id)] = time.monotonic() + CHECK_INTERVAL_SECONDS
                continue

            # Facebook UI và comments.summary.total_count có thể lệch nhau.
            # Deep-count trước khi xét threshold/soạn Telegram cho mọi bài có khả năng đạt/gần ngưỡng.
            # Khi pagination hoàn tất, biến comments được thay bằng TOP_LEVEL; vì vậy cả threshold
            # VÀ dòng "Comments:" trong Telegram đều dùng cùng một số top_level sát Facebook.
            summary_comments = int(comments or 0)
            if should_deep_count_comments(views or 0, summary_comments):
                effective_comments, comment_debug = get_comment_count_fallback(
                    post_id, page_token, summary_comments
                )
                if effective_comments > summary_comments:
                    print(
                        f"[COMMENT-FALLBACK] [{page_name}] {post_id}: "
                        f"summary={summary_comments} -> top_level={effective_comments}"
                    )
                comments = effective_comments

            poll_seconds = _adaptive_poll_seconds(views or 0, comments)
            _post_next_check[str(post_id)] = time.monotonic() + poll_seconds
            print(
                f"[{ts}] [{page_name}] {post_id} -> views={views} | comments={comments}"
                + (f" (summary={summary_comments})" if comments != summary_comments else "")
                + f" | next={poll_seconds}s"
            )

            # Luôn ghi lịch sử views để khi bật công tắc spike đã có baseline.
            # Cảnh báo "BÀI ĐANG DỰNG ĐỨNG" chỉ chạy khi ENABLE_SPIKE_ALERT=true,
            # độc lập với ngưỡng BÀI ĐANG LÊN, gửi đúng 1 lần/post nhờ key spike_ lưu persistent.
            spike_detected = record_and_check_spike(post_id, int(views or 0), datetime.now())
            spike_key = f"spike_{page_id}_{post_id}"
            if ENABLE_SPIKE_ALERT and spike_detected and spike_key not in already_notified:
                # Spike cũng phải kiểm tra link TRƯỚC khi báo Telegram.
                spike_link_status = post_already_has_link(post_id, page_id, page_token, message)
                if spike_link_status is True:
                    print(f"[SPIKE-SKIP-LINK] [{page_name}] {post_id}: đang dựng đứng nhưng đã có link -> KHÔNG BÁO.")
                elif spike_link_status is None:
                    print(f"[SPIKE-BLOCKED] [{page_name}] {post_id}: chưa xác minh được link -> KHÔNG BÁO, thử lại vòng sau.")
                    _post_next_check[str(post_id)] = time.monotonic() + CHECK_INTERVAL_SECONDS
                else:
                    evidence = spike_evidence.get(str(post_id), {})
                    spike_msg = (
                        f"🚀 BÀI ĐANG DỰNG ĐỨNG! (Page: {page_name})\n"
                        f"Views: {views}\n"
                        f"Comments: {comments}\n"
                        + (f"Link: {link}\n" if link else "")
                        + f"Đo thực tế: {evidence.get('baseline_views', 0):,} → {int(views or 0):,} views "
                          f"trong {evidence.get('minutes', '?')} phút.\n"
                        + f"Tăng: +{evidence.get('increase', 0):,} "
                          f"({evidence.get('percent', '?')}%), "
                          f"{evidence.get('rate', '?')} views/phút.\n"
                        + "=> Kiểm tra bài ngay!"
                    )
                    print(f"[SPIKE] [{page_name}] {post_id}: dựng đứng + CHƯA có link -> gửi Telegram.")
                    if send_telegram_message(spike_msg):
                        already_notified.add(spike_key)
                        save_notified(already_notified)
                        changed = True
                    else:
                        print(f"[SPIKE-TELEGRAM-FAILED] [{page_name}] {post_id}: chưa đánh dấu đã báo; sẽ thử lại.")
                        _post_next_check[str(post_id)] = time.monotonic() + CHECK_INTERVAL_SECONDS

            if not meets_threshold(views or 0, comments):
                continue

            # Log rõ ràng để biết chính xác bài đã đạt ngưỡng ở vòng quét nào.
            qualified_reasons = []
            if comments > COMMENT_ONLY_THRESHOLD:
                qualified_reasons.append(f"comments>{COMMENT_ONLY_THRESHOLD}")
            for rule in THRESHOLD_RULES:
                if (views or 0) >= rule["min_views"] and comments >= rule["min_comments"]:
                    qualified_reasons.append(
                        f"views>={rule['min_views']} & comments>={rule['min_comments']}"
                    )
            print(
                f"[QUALIFIED] [{page_name}] {post_id} | views={views} | comments={comments} "
                f"| reason={'; '.join(qualified_reasons) or 'threshold'}"
            )

            # QUAN TRỌNG: luôn kiểm tra link TRƯỚC khi tin trạng thái already_notified.
            # Các phiên bản cũ từng ghi cả bài "đã có link" vào notified_posts.json,
            # làm notified bị lẫn giữa "đã gửi Telegram" và "chỉ bị skip vì có link".
            # Vì vậy notified cũ không được phép chặn một bài đạt ngưỡng trước khi link được xác minh.
            link_status = post_already_has_link(post_id, page_id, page_token, message)
            if link_status is not None:
                resolve_error_alert(f"qualified_link_check_{post_id}", f"Bài {post_id} đã kiểm tra được comment/link.")
            if link_status is None:
                print(
                    f"[QUALIFIED-BUT-BLOCKED] [{page_name}] {post_id}: "
                    f"đã đạt ngưỡng nhưng KHÔNG xác minh được comments/link; "
                    f"không gọi OpenAI để tránh tạo trùng."
                )
                send_error_alert(
                    f"qualified_link_check_{post_id}",
                    f"Bài {post_id} trên Page {page_name} ĐÃ ĐẠT NGƯỠNG "
                    f"(views={views}, comments={comments}) nhưng Facebook API không cho "
                    f"xác minh đầy đủ comments/link. Bot tạm chặn thông báo + OpenAI để "
                    f"tránh tạo TXT trùng. Hãy xem log [QUALIFIED-BUT-BLOCKED] để biết bài nào bị giữ."
                )
                # Thử lại ngay ở vòng quét kế tiếp thay vì chờ adaptive interval.
                _post_next_check[str(post_id)] = time.monotonic() + CHECK_INTERVAL_SECONDS
                continue
            if link_status:
                # Có link thì chỉ SKIP. KHÔNG ghi vào already_notified nữa.
                # notified phải chỉ mang nghĩa "Telegram alert thực sự đã được gửi".
                print(f"[{ts}] [{page_name}] {post_id}: đã có link rồi, bỏ qua không báo.")
                continue

            # Tới đây đã xác minh chắc chắn: bài đạt ngưỡng + CHƯA có link.
            # Xử lý notified sau link-check để sửa dữ liệu legacy bị ô nhiễm.
            if key in already_notified or key in load_notified():
                if str(post_id) in story_completed:
                    print(f"[QUALIFIED] {post_id}: đã gửi đủ TXT -> SKIP.")
                    already_notified.add(key)
                    continue

                progress = _load_progress(post_id)
                has_saved_progress = bool(progress and (
                    progress.get("parts") or progress.get("sent") or progress.get("caption")
                ))

                if has_saved_progress:
                    # Đây là dấu hiệu mạnh rằng alert trước đó đã thật sự đi qua worker.
                    already_notified.add(key)
                    if _retry_due(post_id):
                        retry_msg = (
                            f"🔥 BÀI ĐANG LÊN! (Page: {page_name})\n"
                            f"Views: {views}\nComments: {comments}\n"
                            + (f"Link: {link}\n" if link else "")
                            + "=> Gắn link ngay!\n"
                            + "🔄 Tiếp tục tạo/gửi các file TXT còn thiếu..."
                        )
                        queued = enqueue_story(
                            page_name, post_id, message, page_id, page_token,
                            retry_msg, key
                        )
                        print(
                            f"[RESUME] {post_id}: có checkpoint thật | queued={queued} "
                            f"| parts={sorted((progress.get('parts') or {}).keys())} "
                            f"| sent={progress.get('sent') or []}"
                        )
                    else:
                        print(f"[RESUME-WAIT] {post_id}: có checkpoint nhưng chưa tới retry_after.")
                    continue

                # Legacy repair:
                # notified nhưng KHÔNG completed và KHÔNG có checkpoint thường là key
                # do bản cũ ghi nhầm khi gặp bài đã có link. Xóa key đó để bài được báo bình thường.
                print(
                    f"[LEGACY-NOTIFIED-FIX] {post_id}: có notified cũ nhưng không có "
                    f"story checkpoint/completed; coi là trạng thái legacy không đáng tin "
                    f"và cho phép bài đi tiếp."
                )
                already_notified.discard(key)
                disk_notified = load_notified()
                if key in disk_notified:
                    disk_notified.discard(key)
                    save_notified(disk_notified)
                changed = True

            msg = (
                f"🔥 BÀI ĐANG LÊN! (Page: {page_name})\n"
                f"Views: {views}\n"
                f"Comments: {comments}\n"
                + (f"Link: {link}\n" if link else "")
                + "=> Gắn link ngay!\n"
                + "⏳ Đang tạo file TXT Part 2, 3 & 4 cho chính bài này..."
            )
            # Chặn bài đã bắt đầu tạo và không xếp hàng khi chế độ trả phí đang tắt.
            if not _retry_due(post_id) or not ENABLE_PAID_GENERATION:
                continue
            # Worker gửi thông báo ngay trước khi tạo/gửi 3 TXT; scanner không gửi chen.
            queued = enqueue_story(page_name, post_id, message, page_id, page_token, msg, key)
            if queued:
                print(f"[QUEUE] [{page_name}] {post_id}: đã xếp hàng thông báo + TXT.")
            else:
                print(
                    f"[QUEUE-BUSY] [{page_name}] {post_id}: đạt ngưỡng nhưng chưa vào queue "
                    f"(pending/completed/retry-delay/queue-full). Sẽ được xét lại vòng sau."
                )
                # Đừng để adaptive polling kéo dài lần thử lại của một bài đã đạt ngưỡng.
                _post_next_check[str(post_id)] = time.monotonic() + CHECK_INTERVAL_SECONDS

    if changed:
        save_notified(already_notified)
    save_view_history(view_history)


def main():
    print("Bắt đầu theo dõi bài viết trên tất cả các Page... (Ctrl+C để dừng)")
    if PUBLISH_PLATFORM == "wordpress":
        threading.Thread(target=_wp_repair_requested_once, name="wp-repair", daemon=True).start()
    threading.Thread(target=story_worker, name="story-worker", daemon=True).start()
    if ENABLE_WEB_PUBLISH_TEST and ENABLE_AUTO_WEB_PUBLISH:
        threading.Thread(target=auto_publish_recovery_worker, name="auto-web-recovery", daemon=True).start()
    while True:
        scan_started = time.monotonic()
        try:
            check_all_pages()
            resolve_error_alert("main_loop_exception", "Vong quet Facebook da chay thanh cong.")
        except Exception as e:
            print(f"[LỖI] Lỗi không xác định trong vòng quét: {e}")
            send_error_alert("main_loop_exception", f"Lỗi không xác định trong vòng quét:\n{e}")
        time.sleep(max(0, CHECK_INTERVAL_SECONDS - (time.monotonic() - scan_started)))


if __name__ == "__main__":
    main()
