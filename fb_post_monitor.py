"""
FB POST MONITOR (RETRY + BATCH API + CẢNH BÁO TOKEN + AUTO-REPLY)
=================================================================
Theo dõi tất cả (hoặc 1 phần) Page Facebook bạn quản lý. Gửi thông báo
Telegram khi 1 bài đạt ngưỡng (OR nhiều điều kiện) HOẶC có dấu hiệu
tăng đột biến ("dựng đứng"). Bỏ qua bài đã có link. Tự báo lỗi qua
Telegram (token hỏng, mất mạng...). Tự cảnh báo trước khi token hết hạn.
Tự động trả lời bình luận độc giả sau khi bài đã gắn link (1 độc giả
chỉ được trả lời 1 lần/bài, câu trả lời chọn ngẫu nhiên trong danh sách
mẫu bạn chuẩn bị sẵn). Báo Telegram khi đã trả lời HẾT bình luận hiện có.

BẢN NÀY THÊM: BÁO KHI ĐÃ REPLY HẾT COMMENTS
-----------------------------------------------
Khi bot đã trả lời hết toàn bộ bình luận độc giả hiện có trên 1 bài
(không còn ai bị bỏ sót), gửi 1 tin nhắn Telegram xác nhận riêng — chỉ
báo đúng 1 lần/bài để bạn kiểm tra thử chất lượng câu trả lời.

⚠️ QUYỀN CẦN THÊM: tính năng auto-reply cần quyền GHI (không chỉ đọc)
vào bình luận, cụ thể là "pages_manage_engagement". Khi lấy
USER_ACCESS_TOKEN ở Graph API Explorer, nhớ tick thêm quyền này.

YÊU CẦU
--------
1. USER ACCESS TOKEN (Long-Lived) với đủ 5 quyền: pages_show_list,
   pages_read_engagement, pages_read_user_content, read_insights,
   pages_manage_engagement
2. Telegram Bot Token + Chat ID
3. Cài thư viện: pip install requests
"""

import json
import os
import random
import re
import time
import requests
from datetime import datetime, timezone, timedelta

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "EAAgnXcXSwJUBSeeogSeacmyxvQdq1xNfQbXvSPUTFNmXMtRevU6XTOAcqV8JpmU3th9RwCY94Fz5MhgLGmvsToieBiBeHXHI23lWtU8Foc5XjK2iE3psQ862PPnfQmZBqZCvNAiILg1Ldp1JNHMgqaHhJJwUEIEjjdrhpZCO39ZBhXqgwyDHc2Dmz1XH15DGZCuOG8VyAZBBIr7D5F")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8770004220:AAEUuMts84bq8XUn6Tbyc_qYGOx0F_UZoEw")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7513038171")

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
THRESHOLD_RULES = [
    {"min_views": 4500, "min_comments": 20},
    {"min_views": 3500, "min_comments": 100},
]
COMMENT_ONLY_THRESHOLD = 100  # comments vượt mốc này thì báo luôn, không cần xét views

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 72

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

# --- Auto-reply bình luận sau khi bài đã gắn link ---
ENABLE_AUTO_REPLY = True

# Danh sách câu trả lời mẫu CÓ chèn tên độc giả — {name} sẽ được thay bằng
# tên (first name) của người bình luận khi lấy được.
REPLY_TEMPLATES_WITH_NAME = [
    "Hi {name}! The full story has now been updated in the comments below. Thank you for reading! ❤️",
    "Thanks for reading, {name}! All parts of the story are available in the comment section now. 📖",
    "Hi {name}, the complete story has been posted below in the comments — you can continue reading there! 👇",
    "{name}, the entire story is now updated in the comments. Hope you enjoy it! ❤️",
    "Thanks so much, {name}! The full continuation and ending are now available in the comments. 💕",
    "Hey {name}, the complete story has just been updated in the comment section below! 📚",
]

# Danh sách dự phòng KHÔNG có tên — dùng khi không lấy được tên độc giả,
# hoặc được chọn xen kẽ ngẫu nhiên để tăng độ đa dạng, giảm khả năng bị
# Facebook coi là spam (do lặp lại y hệt quá nhiều).
REPLY_TEMPLATES_NO_NAME = [
    "The full story has now been updated in the comments below. Thank you for reading! ❤️",
    "All parts of the story are available in the comment section now. Hope you enjoy the ending! 📖",
    "The complete story has been posted below in the comments — you can continue reading there! 👇",
    "For those asking for the rest of the story — it's all there now! Scroll through the comments to read the complete version. 👇",
    "You don't have to wait anymore — all remaining parts have been added to the comment section! ✨",
    "The story is officially complete! Check the comments below to read everything from beginning to end. 👇",
    "Wondering what happens next? The complete story has just been updated in the comment section below! 📚",
    "Good news! Every part, including the final ending, has now been posted in the comments. Enjoy! ❤️",
]

MAX_REPLIES_PER_SCAN = 8  # giảm từ 20 xuống 8 để phù hợp với thời gian nghỉ dài hơn giữa các reply
REPLY_DELAY_MIN_SECONDS = 20  # nghỉ ngẫu nhiên giữa mỗi lần reply, tránh nhịp độ đều đặn dễ bị phát hiện là bot
REPLY_DELAY_MAX_SECONDS = 45
REPLIED_COMMENTERS_FILE = "/data/replied_commenters.json"  # lưu danh sách đã reply (post_id_commenter_id)

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
already_spike_notified = set()


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


# -------------------- Auto-reply bình luận độc giả --------------------
def load_replied_commenters():
    if os.path.exists(REPLIED_COMMENTERS_FILE):
        try:
            with open(REPLIED_COMMENTERS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_replied_commenters(replied_set):
    try:
        os.makedirs(os.path.dirname(REPLIED_COMMENTERS_FILE) or ".", exist_ok=True)
        with open(REPLIED_COMMENTERS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(replied_set), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LỖI] Không lưu được {REPLIED_COMMENTERS_FILE}: {e}")


already_replied_commenters = load_replied_commenters()  # tập hợp "post_id_commenter_id"


def get_stream_comments(post_id: str, page_token: str, max_comments: int = 2000):
    """Lấy TOÀN BỘ bình luận (id, message, from) của 1 bài viết, tự động
    phân trang cho tới khi hết hoặc đạt max_comments (để tránh vòng lặp
    vô hạn nếu bài có quá nhiều bình luận)."""
    all_comments = []
    url = f"{GRAPH_URL}/{post_id}/comments"
    params = {"filter": "stream", "limit": 100, "fields": "id,message,from", "access_token": page_token}

    while url and len(all_comments) < max_comments:
        try:
            r = api_get(url, params=params)
        except Exception as e:
            print(f"[LỖI] Lỗi mạng khi lấy comments của {post_id}: {e}")
            break

        data = r.json()
        if "error" in data:
            break

        all_comments.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")
        url = next_url
        params = None  # next_url đã có sẵn đầy đủ query string (kể cả access_token)

    return all_comments


def get_post_permalink(post_id: str, page_token: str) -> str:
    """Lấy link thật của bài viết (dùng khi báo 'đã trả lời hết bình luận')."""
    try:
        r = api_get(
            f"{GRAPH_URL}/{post_id}",
            params={"fields": "permalink_url", "access_token": page_token},
        )
        data = r.json()
        return data.get("permalink_url", "")
    except Exception:
        return ""


def build_reply_text(commenter_name: str) -> str:
    """Tạo câu trả lời, ưu tiên chèn tên độc giả nếu có, xen kẽ ngẫu nhiên
    với bản không tên để tăng độ đa dạng (giảm nguy cơ bị Facebook coi
    là spam do lặp lại y hệt quá nhiều)."""
    first_name = ""
    if commenter_name:
        parts = commenter_name.strip().split()
        if parts:
            first_name = parts[0]

    if first_name and random.random() < 0.7:  # 70% cơ hội dùng bản có tên nếu lấy được tên
        template = random.choice(REPLY_TEMPLATES_WITH_NAME)
        return template.format(name=first_name)

    return random.choice(REPLY_TEMPLATES_NO_NAME)


def reply_to_comment(comment_id: str, text: str, page_token: str) -> bool:
    try:
        resp = api_post(
            f"{GRAPH_URL}/{comment_id}/comments",
            data={"message": text, "access_token": page_token},
        )
        data = resp.json()
        if "id" not in data:
            err_msg = data.get("error", {}).get("message", "Không rõ nguyên nhân")
            print(f"[LỖI] Không reply được comment {comment_id}: {err_msg}")
            send_error_alert(
                "auto_reply_failed",
                f"Không tự trả lời bình luận được (comment {comment_id}).\n"
                f"Chi tiết: {err_msg}\n"
                "Có thể thiếu quyền 'pages_manage_engagement' -> cần lấy lại token với quyền này.",
            )
            return False
        return True
    except Exception as e:
        print(f"[LỖI] Lỗi mạng khi reply comment {comment_id}: {e}")
        return False


def process_auto_replies_for_post(post_id: str, page_id: str, page_token: str, post_message: str, page_name: str = ""):
    """Nếu bài đã có link, tự trả lời các độc giả CHƯA từng được trả lời.
    Khi đã trả lời HẾT toàn bộ bình luận hiện có (không còn ai bị bỏ sót),
    gửi 1 thông báo Telegram xác nhận riêng (chỉ báo 1 lần/bài).
    Trả về tuple (số reply đã gửi lượt này, có thay đổi trạng thái cần lưu không)."""
    global already_notified

    comments = get_stream_comments(post_id, page_token)
    if not comments:
        return 0, False

    has_link = URL_PATTERN.search(post_message or "") is not None
    if not has_link:
        for c in comments:
            from_id = (c.get("from") or {}).get("id")
            msg = c.get("message", "") or ""
            if from_id == page_id and URL_PATTERN.search(msg):
                has_link = True
                break

    if not has_link:
        return 0, False  # bài chưa gắn link, chưa tới lượt auto-reply

    replies_sent = 0
    real_reader_keys = []  # danh sách key của các bình luận thực sự từ độc giả (không tính Page)

    for c in comments:
        commenter = c.get("from") or {}
        commenter_id = commenter.get("id")
        commenter_name = commenter.get("name", "")
        comment_id = c.get("id")
        if not commenter_id or not comment_id:
            continue
        if commenter_id == page_id:
            continue  # bỏ qua bình luận của chính Page (ví dụ bình luận gắn link)

        key = f"{post_id}_{commenter_id}"
        real_reader_keys.append(key)

        if key in already_replied_commenters:
            continue  # độc giả này đã được trả lời rồi, không reply lần 2
        if replies_sent >= MAX_REPLIES_PER_SCAN:
            continue  # đạt giới hạn reply/lượt quét, để dành cho lượt sau

        reply_text = build_reply_text(commenter_name)
        ok = reply_to_comment(comment_id, reply_text, page_token)
        if ok:
            already_replied_commenters.add(key)
            replies_sent += 1
            time.sleep(random.uniform(REPLY_DELAY_MIN_SECONDS, REPLY_DELAY_MAX_SECONDS))

    # --- Kiểm tra đã trả lời HẾT chưa, báo Telegram 1 lần duy nhất nếu đúng ---
    state_changed = replies_sent > 0
    caughtup_key = f"caughtup_{post_id}"
    if real_reader_keys and caughtup_key not in already_notified:
        all_replied = all(k in already_replied_commenters for k in real_reader_keys)
        if all_replied:
            permalink = get_post_permalink(post_id, page_token)
            send_telegram_message(
                f"✅ ĐÃ TRẢ LỜI HẾT BÌNH LUẬN!\n"
                f"Page: {page_name}\n"
                + (f"Link: {permalink}\n" if permalink else f"Post ID: {post_id}\n")
                + f"Tổng số độc giả đã trả lời: {len(set(real_reader_keys))}\n"
                "=> Kiểm tra thử xem câu trả lời có ổn không nhé!"
            )
            already_notified.add(caughtup_key)
            state_changed = True

    return replies_sent, state_changed


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
            if not (f"{page_id}_{pid}" in already_notified and f"{page_id}_{pid}" in already_spike_notified)
        ]

        if post_ids_to_check:
            stats = get_stats_batch(post_ids_to_check, page_token)

            for post_id in post_ids_to_check:
                key = f"{page_id}_{post_id}"
                views, comments, link, message = stats.get(post_id, (None, None, None, ""))

                if views is None or comments is None:
                    print(f"[{ts}] [{page_name}] {post_id}: không lấy được dữ liệu, bỏ qua.")
                    continue

                print(f"[{ts}] [{page_name}] {post_id} -> views={views} | comments={comments}")

                # --- Kiểm tra dấu hiệu "dựng đứng" ---
                is_spike = record_and_check_spike(post_id, views, now_dt)
                if is_spike and key not in already_spike_notified:
                    spike_msg = (
                        f"📈 BÀI ĐANG BÙNG NỔ! (Page: {page_name})\n"
                        f"Post ID: {post_id}\n"
                        f"Views hiện tại: {views} (tăng đột biến trong {SPIKE_LOOKBACK_MINUTES} phút gần nhất)\n"
                        f"Comments: {comments}\n"
                        + (f"Link: {link}\n" if link else "")
                        + "=> Theo dõi sát, chuẩn bị gắn link!"
                    )
                    send_telegram_message(spike_msg)
                    already_spike_notified.add(key)
                    changed = True
                    print(f"[{ts}] Đã báo SPIKE cho [{page_name}] {post_id}")

                if not meets_threshold(views, comments) or key in already_notified:
                    pass  # vẫn tiếp tục xuống dưới để xét auto-reply, không "continue" luôn nữa
                elif post_already_has_link(post_id, page_id, page_token, message):
                    print(f"[{ts}] [{page_name}] {post_id}: đã có link rồi, bỏ qua không báo.")
                    already_notified.add(key)
                    changed = True
                else:
                    msg = (
                        f"🔥 BÀI ĐANG LÊN! (Page: {page_name})\n"
                        f"Post ID: {post_id}\n"
                        f"Views: {views}\n"
                        f"Comments: {comments}\n"
                        + (f"Link: {link}\n" if link else "")
                        + "=> Gắn link ngay!"
                    )
                    send_telegram_message(msg)
                    already_notified.add(key)
                    changed = True
                    print(f"[{ts}] Đã gửi thông báo Telegram cho [{page_name}] {post_id}")

                # --- Auto-reply bình luận độc giả (chạy cho MỌI bài, không chỉ bài vừa đạt ngưỡng) ---
                if ENABLE_AUTO_REPLY:
                    n_replied, reply_state_changed = process_auto_replies_for_post(post_id, page_id, page_token, message, page_name)
                    if n_replied > 0:
                        print(f"[{ts}] [{page_name}] {post_id}: đã tự trả lời {n_replied} độc giả mới.")
                    if reply_state_changed:
                        changed = True

        # Auto-reply cho cả những bài đã "báo xong cả 2 loại" (bị loại khỏi post_ids_to_check
        # ở trên) — các bài này không còn được lấy stats nữa, nhưng vẫn cần tiếp tục auto-reply
        # cho độc giả mới bình luận, cho đến khi bài quá cũ (ngoài ONLY_POSTS_NEWER_THAN_HOURS).
        if ENABLE_AUTO_REPLY:
            already_processed_ids = set(post_ids_to_check)
            for post_id in all_post_ids:
                if post_id in already_processed_ids:
                    continue  # đã xử lý ở vòng lặp trên rồi
                n_replied, reply_state_changed = process_auto_replies_for_post(post_id, page_id, page_token, "", page_name)
                if n_replied > 0:
                    print(f"[{ts}] [{page_name}] {post_id}: đã tự trả lời {n_replied} độc giả mới.")
                if reply_state_changed:
                    changed = True

    if changed:
        save_notified(already_notified)
        save_replied_commenters(already_replied_commenters)
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
