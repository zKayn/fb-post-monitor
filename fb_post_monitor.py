"""
FB POST MONITOR (RETRY + BATCH API + CẢNH BÁO TOKEN + AUTO PART 2/3)
=================================================================
Theo dõi tất cả (hoặc 1 phần) Page Facebook bạn quản lý. Gửi thông báo
Telegram khi 1 bài đạt ngưỡng (OR nhiều điều kiện) HOẶC có dấu hiệu
tăng đột biến ("dựng đứng"). Bỏ qua bài đã có link. Tự báo lỗi qua
Telegram (token hỏng, mất mạng...). Tự cảnh báo trước khi token hết hạn.
Khi 1 bài đạt ngưỡng và CHƯA có link, tự dùng OpenAI API viết tiếp
Part 2 + Part 3 dựa trên caption gốc, xuất ra file Word, gửi kèm
thông báo Telegram.

YÊU CẦU
--------
1. USER ACCESS TOKEN (Long-Lived) với đủ 4 quyền: pages_show_list,
   pages_read_engagement, pages_read_user_content, read_insights
2. Telegram Bot Token + Chat ID
3. OpenAI API Key (lấy tại platform.openai.com/api-keys, cần nạp tiền
   riêng, khác với ChatGPT Plus)
4. Cài thư viện: pip install requests python-docx
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# --- Tự viết Part 2 + Part 3 bằng OpenAI khi bài đạt ngưỡng và CHƯA có link ---
ENABLE_STORY_CONTINUATION = True
OPENAI_MODEL = "gpt-5.6-sol"
OPENAI_MAX_OUTPUT_TOKENS = 8000  # đủ cho ~2x 3500-4000 từ theo yêu cầu prompt
OPENAI_TIMEOUT_SECONDS = 300  # sinh văn bản dài có thể mất vài phút

STORY_PROMPT_TEMPLATE = """You are a professional storyteller and novelist capable of crafting emotionally resonant works that emphasize character depth and captivate a wide audience.

Please continue by writing Part 2 and Part 3 of the story provided above.

Requirements:
*   Divide the text into two sections: PART 2 and PART 3; present them as separate blocks for easy copying.
*   Write in a natural, engaging novelistic style with a smooth rhythm and compelling storytelling.

*   Focus on authentic characters, emotional depth, family relationships, personal growth, trust, forgiveness, resilience, and meaningful life choices.

*   Build suspense through hidden truths, difficult decisions, misunderstandings, subtle clues, and gradual revelations, rather than through heated confrontations or sensational events.

*   Maintain reader curiosity through authentic dialogue, multi-dimensional character motivations, emotional conflicts, and surprising yet logical discoveries.

*   Ensure all events stem from a solid and convincing emotional foundation.

*   Avoid elements of graphic violence, physical abuse, domestic violence, cruelty, intimidation, revenge, public humiliation, excessive threats, manipulation, or abuse of power.

*   Avoid scenes that are shocking, psychologically traumatic, or emotionally overwhelming for children or vulnerable characters. If children appear, portray them in safe, supported, and age-appropriate situations.

*   Avoid sensational plot twists intended solely for shock value. Instead, build drama through mystery, relationships, and meaningful character choices.

• Use vivid descriptions, authentic dialogue, and emotionally resonant storytelling to engage a broad audience.

• Give supporting characters meaningful roles, realistic motivations, and emotional growth.

• Maintain a warm, family-friendly tone suitable for mass-market publishing platforms and ad-friendly content standards.

Structure:

• Length: Approximately 3,500–4,000 words per part.

• Follow naturally from Part 1 while maintaining consistency in characters, setting, and timeline.

• Gradually reveal new information to deepen the mystery and strengthen emotional bonds between characters.

• Conclude Part 2 with a logical reveal, a compelling open-ended question, or a significant discovery that naturally leads the reader into Part 3, without relying on shock value, violence, or intense conflict. Output format:

1. Always create a concise, engaging title that sparks curiosity and highlights the emotional journey, family relationships, hidden truths, or meaningful choices, while avoiding sensationalist or misleading language. 2. Write the full story for Part 2.

3. End exactly with the following lines (IN BOLD):
-END OF PART 2 – CLICK THE "NEXT PART" SECTION AT THE BOTTOM OF THE PAGE TO CONTINUE READING
-END OF PART 3 – PLEASE "LIKE" AND SHARE THIS POST ON FACEBOOK TO SUPPORT US IN SHARING EVEN MORE STORIES"""

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
THRESHOLD_RULES = [
    {"min_views": 7000, "min_comments": 20},
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

# --- Retry khi gặp lỗi mạng tạm thời ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3  # tăng dần: 3s, 6s, 9s giữa các lần thử lại

# --- Batch API ---
# Mỗi bài viết cần 3 sub-request: 2 request insights riêng biệt (giữ cơ chế
# fallback metric để không mất dữ liệu nếu 1 metric không áp dụng được cho
# bài đó) + 1 request lấy message/comments/link. 16 bài x 3 = 48, dưới giới
# hạn tối đa 50 sub-request/batch của Facebook.
POSTS_PER_BATCH = 16
METRIC_FALLBACKS = ["post_media_view", "post_total_media_view_unique"]

# --- Cảnh báo token sắp hết hạn ---
TOKEN_EXPIRY_WARNING_DAYS = 5

CHECK_INTERVAL_SECONDS = 600
GRAPH_API_VERSION = "v20.0"
NOTIFIED_FILE = "/data/notified_posts.json"
# ==============================================================

GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)


# ==================== RETRY HELPER ====================
def api_get(url, params=None, timeout=20):
    """GET request có tự động retry khi gặp lỗi mạng tạm thời."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return requests.get(url, params=params, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[CẢNH BÁO] Lỗi mạng (lần {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise last_exc


def api_post(url, data=None, timeout=30):
    """POST request có tự động retry khi gặp lỗi mạng tạm thời."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return requests.post(url, data=data, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[CẢNH BÁO] Lỗi mạng (lần {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
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
def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = api_post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        if resp.status_code != 200:
            print(f"[LỖI] Gửi Telegram thất bại: {resp.text}")
    except Exception as e:
        print(f"[LỖI] Gửi Telegram thất bại (mạng): {e}")


def send_telegram_document(file_path: str, caption: str = ""):
    """Gửi 1 file đính kèm (VD file Word) qua Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024]},
                files={"document": f},
                timeout=120,
            )
        if resp.status_code != 200:
            print(f"[LỖI] Gửi file Telegram thất bại: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"[LỖI] Lỗi mạng khi gửi file Telegram: {e}")
        return False


# -------------------- Tự viết Part 2 + Part 3 (OpenAI) --------------------
def call_openai_story(caption: str):
    """Gọi OpenAI API để viết Part 2 + Part 3 dựa trên caption gốc.
    Trả về đoạn văn bản kết quả, hoặc None nếu lỗi."""
    if not OPENAI_API_KEY:
        print("[CẢNH BÁO] Chưa cấu hình OPENAI_API_KEY, bỏ qua bước viết Part 2/3.")
        return None

    full_prompt = f"{caption}\n\n{STORY_PROMPT_TEMPLATE}"

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": full_prompt}],
                    "max_tokens": OPENAI_MAX_OUTPUT_TOKENS,
                    "temperature": 0.85,
                },
                timeout=OPENAI_TIMEOUT_SECONDS,
            )
            data = resp.json()
            if "choices" in data and data["choices"]:
                return data["choices"][0]["message"]["content"]
            err_msg = data.get("error", {}).get("message", "Không rõ nguyên nhân")
            print(f"[LỖI] OpenAI API lỗi: {err_msg}")
            send_error_alert("openai_api_error", f"Lỗi khi gọi OpenAI API để viết Part 2/3:\n{err_msg}")
            return None
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[CẢNH BÁO] Lỗi mạng khi gọi OpenAI (lần {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    send_error_alert("openai_api_network", f"Không kết nối được OpenAI sau {MAX_RETRIES} lần thử: {last_exc}")
    return None


def create_story_docx(story_text: str, out_path: str):
    """Tạo file Word từ đoạn văn bản Part 2/3 do OpenAI trả về."""
    from docx import Document

    lines = [l for l in story_text.split("\n")]
    title = next((l.strip() for l in lines if l.strip()), "Story Continuation")

    doc = Document()
    doc.add_heading(title, level=1)
    for line in lines:
        if line.strip():
            doc.add_paragraph(line.strip())
    doc.save(out_path)
    return title


def generate_and_send_story_continuation(page_name: str, post_id: str, caption: str):
    """Toàn bộ luồng: gọi OpenAI -> tạo file Word -> gửi qua Telegram."""
    if not ENABLE_STORY_CONTINUATION:
        return
    if not caption or not caption.strip():
        print(f"[CẢNH BÁO] Bài {post_id} không có caption, bỏ qua viết Part 2/3.")
        return

    print(f"[OPENAI] Đang viết Part 2/3 cho bài {post_id}...")
    story_text = call_openai_story(caption)
    if not story_text:
        return

    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", post_id)
    docx_path = f"/tmp/story_{safe_id}.docx"
    try:
        title = create_story_docx(story_text, docx_path)
    except Exception as e:
        print(f"[LỖI] Không tạo được file Word: {e}")
        send_error_alert("story_docx_error", f"Không tạo được file Word cho Part 2/3 bài {post_id}: {e}")
        return

    ok = send_telegram_document(
        docx_path,
        caption=f"📖 Part 2 & 3 tự động — Page: {page_name}\n{title[:200]}",
    )
    if ok:
        print(f"[OPENAI] Đã gửi file Part 2/3 cho bài {post_id}.")

    try:
        os.remove(docx_path)
    except Exception:
        pass


ERROR_COOLDOWN_MINUTES = 30
_last_error_sent_at = {}


def send_error_alert(error_key: str, text: str):
    now = datetime.now()
    last_sent = _last_error_sent_at.get(error_key)
    if last_sent and (now - last_sent).total_seconds() < ERROR_COOLDOWN_MINUTES * 60:
        return
    send_telegram_message(f"⚠️ LỖI SCRIPT!\n{text}\n\nThời gian: {now.strftime('%H:%M:%S %d/%m/%Y')}")
    _last_error_sent_at[error_key] = now


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


# -------------------- Facebook API --------------------
def get_managed_pages():
    pages = []
    url = f"{GRAPH_URL}/me/accounts"
    params = {"fields": "id,name,access_token", "limit": 100, "access_token": USER_ACCESS_TOKEN}
    while url:
        try:
            r = api_get(url, params=params)
        except Exception as e:
            send_error_alert("get_managed_pages_network", f"Không kết nối được Facebook để lấy danh sách Page: {e}")
            return []
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
    try:
        r = api_get(
            f"{GRAPH_URL}/{page_id}/posts",
            params={"fields": "id,created_time", "limit": 25, "access_token": page_token},
        )
    except Exception as e:
        send_error_alert(f"get_recent_post_ids_net_{page_id}", f"Page ID {page_id}: lỗi mạng khi lấy danh sách bài viết: {e}")
        return []

    data = r.json()
    if "error" in data:
        err_msg = data["error"].get("message", "Không rõ nguyên nhân")
        print(f"[LỖI] Page {page_id}: không lấy được danh sách bài viết: {err_msg}")
        send_error_alert(
            f"get_recent_post_ids_{page_id}",
            f"Page ID {page_id}: không lấy được danh sách bài viết (có thể token của Page bị lỗi).\nChi tiết: {err_msg}",
        )
        return []

    now = datetime.now(timezone.utc)
    post_ids = []
    for post in data.get("data", []):
        created = post.get("created_time")
        pid = post.get("id")
        if not created or not pid:
            continue
        created_dt = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S%z")
        age_hours = (now - created_dt).total_seconds() / 3600
        if age_hours <= ONLY_POSTS_NEWER_THAN_HOURS:
            post_ids.append(pid)
    return post_ids


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
            send_error_alert("batch_request_network", f"Lỗi mạng khi gọi Batch API: {e}")
            continue

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


def post_already_has_link(post_id: str, page_id: str, page_token: str, post_message: str) -> bool:
    if URL_PATTERN.search(post_message or ""):
        return True

    try:
        r = api_get(
            f"{GRAPH_URL}/{post_id}/comments",
            params={"filter": "stream", "limit": 50, "fields": "message,from", "access_token": page_token},
        )
    except Exception:
        return False

    data = r.json()
    if "error" in data:
        return False

    for c in data.get("data", []):
        from_id = (c.get("from") or {}).get("id")
        msg = c.get("message", "") or ""
        if from_id == page_id and URL_PATTERN.search(msg):
            return True
    return False


def check_all_pages():
    check_token_expiry()  # kiểm tra hạn token mỗi lượt quét (rất nhẹ, không đáng kể)

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

        # Chỉ bỏ qua hoàn toàn khi bài đã được báo CẢ 2 loại (ngưỡng chính + spike)
        post_ids_to_check = [
            pid for pid in all_post_ids
            if not (f"{page_id}_{pid}" in already_notified and f"spike_{page_id}_{pid}" in already_notified)
        ]

        if not post_ids_to_check:
            continue

        stats = get_stats_batch(post_ids_to_check, page_token)

        for post_id in post_ids_to_check:
            key = f"{page_id}_{post_id}"
            views, comments, link, message = stats.get(post_id, (None, None, None, ""))

            if views is None or comments is None:
                print(f"[{ts}] [{page_name}] {post_id}: không lấy được dữ liệu, bỏ qua.")
                continue

            print(f"[{ts}] [{page_name}] {post_id} -> views={views} | comments={comments}")

            # --- Kiểm tra dấu hiệu "dựng đứng" ---
            spike_key = f"spike_{key}"
            is_spike = record_and_check_spike(post_id, views, now_dt)
            if is_spike and spike_key not in already_notified:
                spike_has_link = post_already_has_link(post_id, page_id, page_token, message)
                if spike_has_link:
                    # Bài đã gắn link rồi -> không cần báo "đang bùng nổ" nữa, bỏ qua luôn
                    already_notified.add(spike_key)
                    changed = True
                    print(f"[{ts}] [{page_name}] {post_id}: có dấu hiệu spike nhưng đã gắn link rồi, bỏ qua không báo.")
                elif spike_key not in load_notified():  # đọc lại file mới nhất, giảm rủi ro trùng nếu có tiến trình khác vừa ghi
                    spike_msg = (
                        f"📈 ĐANG BÙNG NỔ! (Page: {page_name})\n"
                        f"Views hiện tại: {views} (tăng đột biến trong {SPIKE_LOOKBACK_MINUTES} phút gần nhất)\n"
                        f"Comments: {comments}\n"
                        + (f"Link: {link}\n" if link else "")
                        + "=> CHƯA gắn link, theo dõi sát và chuẩn bị gắn link ngay!"
                    )
                    send_telegram_message(spike_msg)
                    already_notified.add(spike_key)
                    changed = True
                    save_notified(already_notified)  # lưu ngay lập tức sau khi gửi, giảm cửa sổ race-condition
                    print(f"[{ts}] Đã báo SPIKE cho [{page_name}] {post_id}")

            if not meets_threshold(views, comments) or key in already_notified:
                continue

            if post_already_has_link(post_id, page_id, page_token, message):
                print(f"[{ts}] [{page_name}] {post_id}: đã có link rồi, bỏ qua không báo.")
                already_notified.add(key)
                changed = True
                continue

            if key in load_notified():  # đọc lại file mới nhất, giảm rủi ro trùng nếu có tiến trình khác vừa ghi
                print(f"[{ts}] [{page_name}] {post_id}: vừa được báo bởi tiến trình khác, bỏ qua.")
                already_notified.add(key)
                continue

            msg = (
                f"🔥 BÀI ĐANG LÊN! (Page: {page_name})\n"
                f"Views: {views}\n"
                f"Comments: {comments}\n"
                + (f"Link: {link}\n" if link else "")
                + "=> Gắn link ngay!"
            )
            send_telegram_message(msg)
            already_notified.add(key)
            changed = True
            save_notified(already_notified)  # lưu ngay lập tức sau khi gửi, giảm cửa sổ race-condition
            print(f"[{ts}] Đã gửi thông báo Telegram cho [{page_name}] {post_id}")

            # --- Tự viết Part 2 + Part 3 dựa trên caption, gửi kèm file Word ---
            generate_and_send_story_continuation(page_name, post_id, message)

    if changed:
        save_notified(already_notified)
    save_view_history(view_history)


def main():
    print("Bắt đầu theo dõi bài viết trên tất cả các Page... (Ctrl+C để dừng)")
    while True:
        try:
            check_all_pages()
        except Exception as e:
            print(f"[LỖI] Lỗi không xác định trong vòng quét: {e}")
            send_error_alert("main_loop_exception", f"Lỗi không xác định trong vòng quét:\n{e}")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
