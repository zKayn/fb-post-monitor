"""
FB POST MONITOR (CHẠY QUA GITHUB ACTIONS - MIỄN PHÍ VĨNH VIỄN)
=================================================================
Theo dõi tất cả (hoặc 1 phần) Page Facebook bạn quản lý. Gửi thông báo
Telegram khi 1 bài đạt ngưỡng (OR nhiều điều kiện) HOẶC có dấu hiệu
tăng đột biến ("dựng đứng"). Bỏ qua bài đã có link. Tự báo lỗi qua
Telegram (token hỏng, mất mạng...). Tự cảnh báo trước khi token hết hạn.


YÊU CẦU
--------
1. USER ACCESS TOKEN (Long-Lived) với đủ 4 quyền: pages_show_list,
   pages_read_engagement, pages_read_user_content, read_insights
2. Telegram Bot Token + Chat ID
3. Trên GitHub repo: Settings -> Secrets and variables -> Actions,
   thêm 3 secret: USER_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "EAAgnXcXSwJUBSRsR5ZCZBpfGsLffEZCUEYtBzMyDB7cF2LLZChO79rUIU5BCB779Rs7xDaAw6dZBOpUU3n8ZCFKznyoZBJ9E7bydbzrFXsmKzfbgFZBYKyyLZACcvfutHlW4IJq1Kci2qFfM97yNr3jnZBytbjSocCxFy5B8XfSVTUpfFsQDFl4iZAZClkHYTovciRZA9NZBJMxDI21SZBS0gzx")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8770004220:AAEUuMts84bq8XUn6Tbyc_qYGOx0F_UZoEw")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7513038171")

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
THRESHOLD_RULES = [
    {"min_views": 3500, "min_comments": 20},
    {"min_views": 3000, "min_comments": 50},
]
COMMENT_ONLY_THRESHOLD = 60  # comments vượt mốc này thì báo luôn, không cần xét views

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 120

# --- Phát hiện "dựng đứng" (viral spike) dựa trên tốc độ tăng views ---
SPIKE_LOOKBACK_MINUTES = 30
SPIKE_MIN_VIEW_INCREASE = 3000
SPIKE_MIN_PERCENT_INCREASE = 80
SPIKE_MIN_VIEWS_TO_CHECK = 2000
VIEW_HISTORY_FILE = "view_history.json"  # lưu ngay trong repo, KHÔNG dùng /data (GitHub Actions không có Volume)

# --- Retry khi gặp lỗi mạng tạm thời ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

# --- Batch API ---
POSTS_PER_BATCH = 16
METRIC_FALLBACKS = ["post_media_view", "post_total_media_view_unique"]

# --- Cảnh báo token sắp hết hạn ---
TOKEN_EXPIRY_WARNING_DAYS = 5

GRAPH_API_VERSION = "v20.0"
NOTIFIED_FILE = "notified_posts.json"  # lưu ngay trong repo
ERROR_ALERT_STATE_FILE = "error_alert_state.json"  # lưu thời điểm báo lỗi gần nhất, để cooldown hoạt động đúng qua nhiều lần chạy
# ==============================================================

GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)


# ==================== RETRY HELPER ====================
def api_get(url, params=None, timeout=20):
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


def load_error_alert_state():
    if os.path.exists(ERROR_ALERT_STATE_FILE):
        try:
            with open(ERROR_ALERT_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)  # {error_key: iso_timestamp}
        except Exception:
            return {}
    return {}


def save_error_alert_state(state: dict):
    try:
        with open(ERROR_ALERT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LỖI] Không lưu được {ERROR_ALERT_STATE_FILE}: {e}")


_last_error_sent_at = load_error_alert_state()  # {error_key: iso_timestamp}, lưu file để cooldown đúng qua nhiều lần chạy


def send_error_alert(error_key: str, text: str):
    now = datetime.now()
    last_sent_str = _last_error_sent_at.get(error_key)
    if last_sent_str:
        last_sent = datetime.fromisoformat(last_sent_str)
        if (now - last_sent).total_seconds() < ERROR_COOLDOWN_MINUTES * 60:
            return
    send_telegram_message(f"⚠️ LỖI SCRIPT!\n{text}\n\nThời gian: {now.strftime('%H:%M:%S %d/%m/%Y')}")
    _last_error_sent_at[error_key] = now.isoformat()
    save_error_alert_state(_last_error_sent_at)


# -------------------- Cảnh báo token sắp hết hạn --------------------
def check_token_expiry():
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
            "Hãy lấy token mới và cập nhật vào GitHub Secrets ngay.",
        )
        return

    expires_at = data.get("expires_at")
    if not expires_at:
        return

    expire_dt = datetime.fromtimestamp(expires_at)
    days_left = (expire_dt - datetime.now()).days
    if days_left <= TOKEN_EXPIRY_WARNING_DAYS:
        send_error_alert(
            "token_expiry_warning",
            f"USER_ACCESS_TOKEN sắp hết hạn! Còn khoảng {days_left} ngày "
            f"(hết hạn lúc {expire_dt.strftime('%H:%M %d/%m/%Y')}).\n"
            "Hãy lấy Long-Lived Token mới tại Graph API Explorer và cập nhật "
            "vào GitHub -> Settings -> Secrets and variables -> Actions -> USER_ACCESS_TOKEN.",
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
    results = {}

    for group in chunked(post_ids, POSTS_PER_BATCH):
        batch_items = []
        for pid in group:
            for metric in METRIC_FALLBACKS:
                batch_items.append(
                    {"method": "GET", "relative_url": f"{pid}/insights?metric={metric}&period=lifetime"}
                )
            batch_items.append(
                {"method": "GET", "relative_url": f"{pid}?fields=message,comments.summary(true),permalink_url"}
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
        items_per_post = n_metrics + 1

        for idx, pid in enumerate(group):
            base = idx * items_per_post
            insight_items = batch_resp[base : base + n_metrics] if base + n_metrics <= len(batch_resp) else []
            fields_item = batch_resp[base + n_metrics] if base + n_metrics < len(batch_resp) else None

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
    check_token_expiry()

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

        post_ids_to_check = [
            pid for pid in all_post_ids
            if not (f"{page_id}_{pid}" in already_notified and f"{page_id}_{pid}" in already_spike_notified)
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

            is_spike = record_and_check_spike(post_id, views, now_dt)
            if is_spike and key not in already_spike_notified:
                spike_has_link = post_already_has_link(post_id, page_id, page_token, message)
                if spike_has_link:
                    action_line = "=> Bài ĐÃ gắn link rồi, tiếp tục theo dõi đà tăng trưởng."
                else:
                    action_line = "=> CHƯA gắn link, theo dõi sát và chuẩn bị gắn link ngay!"
                spike_msg = (
                    f"📈 BÀI ĐANG BÙNG NỔ! (Page: {page_name})\n"
                    f"Post ID: {post_id}\n"
                    f"Views hiện tại: {views} (tăng đột biến trong {SPIKE_LOOKBACK_MINUTES} phút gần nhất)\n"
                    f"Comments: {comments}\n"
                    + (f"Link: {link}\n" if link else "")
                    + action_line
                )
                send_telegram_message(spike_msg)
                already_spike_notified.add(key)
                changed = True
                print(f"[{ts}] Đã báo SPIKE cho [{page_name}] {post_id} (đã gắn link: {spike_has_link})")

            if not meets_threshold(views, comments) or key in already_notified:
                continue

            if post_already_has_link(post_id, page_id, page_token, message):
                print(f"[{ts}] [{page_name}] {post_id}: đã có link rồi, bỏ qua không báo.")
                already_notified.add(key)
                changed = True
                continue

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

    if changed:
        save_notified(already_notified)
    save_view_history(view_history)


if __name__ == "__main__":
    print("Bắt đầu 1 lượt quét (GitHub Actions)...")
    check_all_pages()
    print("Hoàn tất lượt quét.")
