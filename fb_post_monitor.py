"""
FB POST MONITOR (NHIỀU PAGE, TỰ ĐỘNG, CHẠY 24/7)
=========================================================
Tự động theo dõi TẤT CẢ (hoặc 1 phần) các Page Facebook bạn quản lý.
Với mỗi Page, script lấy danh sách bài viết MỚI NHẤT, kiểm tra views/comments,
và gửi thông báo Telegram ngay khi 1 bài đạt đủ 1 TRONG CÁC điều kiện dưới
đây — kèm tên Page + link.

ĐIỀU KIỆN THÔNG BÁO (OR — đạt 1 trong các điều kiện là báo ngay)
------------------------------------------------------------------
  - >= 5000 views  VÀ  >= 20 comments
  - >= 3500 views  VÀ  >= 100 comments
  - Comments > 100 (bất kể views bao nhiêu)
Chỉnh sửa trong phần THRESHOLD_RULES / COMMENT_ONLY_THRESHOLD bên dưới.

BẢN NÀY THÊM
------------
1. Nhớ trạng thái đã báo qua file "notified_posts.json" -> khởi động lại
   script (deploy lại, restart server...) sẽ KHÔNG báo lại các bài đã báo.
2. Trước khi báo, tự kiểm tra bài viết đã có link (trong nội dung bài hoặc
   trong bình luận của chính Page) hay chưa -> nếu có rồi thì bỏ qua, không báo.

YÊU CẦU TRƯỚC KHI CHẠY
-----------------------
1. USER ACCESS TOKEN (không phải Page Token) tại:
   https://developers.facebook.com/tools/explorer/
   - "User or Page" -> "Get User Access Token"
   - Add a Permission -> tick: pages_show_list, pages_read_engagement,
     pages_read_user_content, read_insights
   - Generate Access Token -> Opt in to all current Pages -> copy token.

2. Telegram Bot: @BotFather -> /newbot -> lấy BOT_TOKEN, lấy CHAT_ID qua
   https://api.telegram.org/bot<BOT_TOKEN>/getUpdates

3. Cài thư viện: pip install requests

CÁCH DÙNG
---------
- Điền USER_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID bên dưới.
- (Tuỳ chọn) INCLUDE_PAGE_NAMES để trống [] = theo dõi TẤT CẢ Page.
- Chạy: python fb_post_monitor.py
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = os.getenv("USER_ACCESS_TOKEN", "EAAgnXcXSwJUBSZA7u2ySKgEB8UGVvSTwEQLNw8jL159ZCn8293Eb7CAa7nnZBu7SPV5N4aje6158kfnMAGZBo4tuzJdhqDEzFEYNz0Mt1ZBrjg2yphct70kyc3dDpmmE0PfkN6hAdxJtLD8HHUIdaZCOKOcRsjadqMh12WH9i7BgJqbooZBDn3GZCJ8fBGPjqtla3pjMfFz8ZBp3CLnIo")

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

# Điều kiện thông báo (OR — chỉ cần đạt 1 trong các điều kiện dưới là báo):
#   - >= 5000 views VÀ >= 20 comments
#   - >= 3500 views VÀ >= 100 comments
#   - Comments > 100 (bất kể views bao nhiêu)
THRESHOLD_RULES = [
    {"min_views": 5000, "min_comments": 20},
    {"min_views": 3500, "min_comments": 100},
]
COMMENT_ONLY_THRESHOLD = 100  # comments vượt mốc này thì báo luôn, không cần xét views

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 72

# --- Phát hiện "dựng đứng" (viral spike) dựa trên tốc độ tăng views ---
# So sánh views hiện tại với views của khoảng SPIKE_LOOKBACK_MINUTES phút trước.
# Nếu tăng đủ nhiều (theo số tuyệt đối HOẶC theo %) -> coi là dấu hiệu "đang lên", báo ngay.
SPIKE_LOOKBACK_MINUTES = 30
SPIKE_MIN_VIEW_INCREASE = 3000   # tăng tối thiểu bấy nhiêu views trong khoảng thời gian trên
SPIKE_MIN_PERCENT_INCREASE = 80  # HOẶC tăng tối thiểu bấy nhiêu % so với mốc trước
SPIKE_MIN_VIEWS_TO_CHECK = 2000  # chỉ bắt đầu xét spike khi views hiện tại >= mốc này (tránh báo nhiễu bài quá mới)
# LƯU Ý: đặt trong /data (Railway Volume) để KHÔNG bị mất lịch sử mỗi khi deploy lại.
VIEW_HISTORY_FILE = "/data/view_history.json"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8770004220:AAEUuMts84bq8XUn6Tbyc_qYGOx0F_UZoEw")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7513038171")

CHECK_INTERVAL_SECONDS = 60  # tần suất kiểm tra (giây)
GRAPH_API_VERSION = "v20.0"

# File lưu lại các bài đã báo/đã có link, để không báo trùng khi restart.
# LƯU Ý: đặt trong /data (Railway Volume) để KHÔNG bị mất dữ liệu mỗi khi deploy lại.
# Nếu chưa tạo Volume trên Railway, có thể tạm để "notified_posts.json" (không có /data/)
# nhưng dữ liệu sẽ mất mỗi lần deploy lại code.
NOTIFIED_FILE = "/data/notified_posts.json"
# ==============================================================

GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
# post_impressions/post_impressions_unique đã bị Facebook deprecated (15/06/2026).
METRIC_FALLBACKS = ["post_media_view", "post_total_media_view_unique"]

URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)


def meets_threshold(views: int, comments: int) -> bool:
    """Trả về True nếu bài đạt ĐỦ 1 trong các điều kiện thông báo đã cấu hình."""
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


already_notified = load_notified()  # tập hợp "page_id_post_id" đã xử lý xong (đạt ngưỡng)
already_spike_notified = set()  # tập hợp bài đã báo "dựng đứng" rồi, tránh báo lặp lại


# -------------------- Lưu/đọc lịch sử views (để phát hiện tăng đột biến) --------------------
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


view_history = load_view_history()  # {post_id: [[iso_timestamp, views], ...]}


def record_and_check_spike(post_id: str, current_views: int, now: datetime):
    """Ghi nhận views hiện tại vào lịch sử, và kiểm tra xem có dấu hiệu
    'dựng đứng' (tăng đột biến trong SPIKE_LOOKBACK_MINUTES phút gần nhất) không.
    Trả về True nếu phát hiện tăng đột biến."""
    points = view_history.get(post_id, [])

    # Tìm điểm dữ liệu gần với mốc SPIKE_LOOKBACK_MINUTES phút trước nhất
    cutoff = now - timedelta(minutes=SPIKE_LOOKBACK_MINUTES)
    baseline_views = None
    for ts_str, v in points:
        ts = datetime.fromisoformat(ts_str)
        if ts <= cutoff:
            baseline_views = v  # lấy điểm gần cutoff nhất (points được lưu theo thứ tự thời gian)
        else:
            break

    # Ghi thêm điểm dữ liệu hiện tại, giữ tối đa 200 điểm gần nhất để file không phình to
    points.append([now.isoformat(), current_views])
    view_history[post_id] = points[-200:]

    if current_views < SPIKE_MIN_VIEWS_TO_CHECK:
        return False  # bài còn quá ít view, chưa đủ ý nghĩa để xét tăng đột biến

    if baseline_views is None:
        return False  # chưa đủ lịch sử (bài quá mới) để so sánh

    increase = current_views - baseline_views
    percent_increase = (increase / baseline_views * 100) if baseline_views > 0 else 0

    return increase >= SPIKE_MIN_VIEW_INCREASE or percent_increase >= SPIKE_MIN_PERCENT_INCREASE


# -------------------- Facebook API --------------------
def get_managed_pages():
    """Lấy danh sách toàn bộ Page bạn quản lý, kèm access token riêng từng Page."""
    pages = []
    url = f"{GRAPH_URL}/me/accounts"
    params = {
        "fields": "id,name,access_token",
        "limit": 100,
        "access_token": USER_ACCESS_TOKEN,
    }
    while url:
        r = requests.get(url, params=params)
        data = r.json()
        if "error" in data:
            err_msg = data['error'].get('message', 'Không rõ nguyên nhân')
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
    """Lấy danh sách ID các bài viết gần đây của 1 Page (trong N giờ)."""
    r = requests.get(
        f"{GRAPH_URL}/{page_id}/posts",
        params={"fields": "id,created_time", "limit": 25, "access_token": page_token},
    )
    data = r.json()
    if "error" in data:
        err_msg = data['error'].get('message', 'Không rõ nguyên nhân')
        print(f"[LỖI] Page {page_id}: không lấy được danh sách bài viết: {err_msg}")
        send_error_alert(
            f"get_recent_post_ids_{page_id}",
            f"Page ID {page_id}: không lấy được danh sách bài viết (có thể token của Page này bị lỗi/hết quyền).\nChi tiết: {err_msg}",
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


def get_post_stats(post_id: str, page_token: str):
    """Trả về (views, comments, link, message) của 1 bài viết."""
    views = None
    for metric in METRIC_FALLBACKS:
        r = requests.get(
            f"{GRAPH_URL}/{post_id}/insights",
            params={"metric": metric, "period": "lifetime", "access_token": page_token},
        )
        data = r.json()
        if "data" in data and data["data"]:
            try:
                views = data["data"][0]["values"][-1]["value"]
                break
            except (KeyError, IndexError):
                continue
        elif "error" in data:
            continue

    r2 = requests.get(
        f"{GRAPH_URL}/{post_id}",
        params={
            "fields": "message,comments.summary(true),permalink_url",
            "access_token": page_token,
        },
    )
    data2 = r2.json()
    comments = None
    link = None
    message = data2.get("message", "") or ""
    if "comments" in data2:
        comments = data2["comments"]["summary"]["total_count"]
        link = data2.get("permalink_url")
    elif "error" in data2:
        err_msg = data2['error'].get('message', 'Không rõ nguyên nhân')
        print(f"[LỖI] {post_id}: {err_msg}")
        send_error_alert(
            f"get_post_stats_{post_id}",
            f"Bài viết {post_id}: lỗi khi đọc dữ liệu (views/comments).\nChi tiết: {err_msg}",
        )

    return views, comments, link, message


def post_already_has_link(post_id: str, page_id: str, page_token: str, post_message: str) -> bool:
    """Kiểm tra bài viết đã có link chưa (trong nội dung bài, hoặc trong
    bình luận do chính Page đăng — trường hợp gắn link bằng comment)."""
    if URL_PATTERN.search(post_message or ""):
        return True

    r = requests.get(
        f"{GRAPH_URL}/{post_id}/comments",
        params={
            "filter": "stream",
            "limit": 50,
            "fields": "message,from",
            "access_token": page_token,
        },
    )
    data = r.json()
    if "error" in data:
        return False  # không chắc chắn -> coi như chưa gắn link, để an toàn vẫn báo

    for c in data.get("data", []):
        from_id = (c.get("from") or {}).get("id")
        msg = c.get("message", "") or ""
        if from_id == page_id and URL_PATTERN.search(msg):
            return True
    return False


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})
    if resp.status_code != 200:
        print(f"[LỖI] Gửi Telegram thất bại: {resp.text}")


# -------------------- Báo lỗi qua Telegram (có chống spam) --------------------
ERROR_COOLDOWN_MINUTES = 30  # mỗi loại lỗi chỉ báo lại sau tối thiểu 30 phút
_last_error_sent_at = {}  # key = loại lỗi, value = thời điểm báo gần nhất


def send_error_alert(error_key: str, text: str):
    """Gửi cảnh báo lỗi qua Telegram, tự chống spam theo error_key."""
    now = datetime.now()
    last_sent = _last_error_sent_at.get(error_key)
    if last_sent and (now - last_sent).total_seconds() < ERROR_COOLDOWN_MINUTES * 60:
        return  # lỗi này vừa báo gần đây rồi, bỏ qua để tránh spam
    send_telegram_message(f"⚠️ LỖI SCRIPT!\n{text}\n\nThời gian: {now.strftime('%H:%M:%S %d/%m/%Y')}")
    _last_error_sent_at[error_key] = now


def check_all_pages():
    pages = get_managed_pages()
    ts = datetime.now().strftime("%H:%M:%S")

    if not pages:
        print(f"[{ts}] Không tìm thấy Page nào (kiểm tra lại USER_ACCESS_TOKEN).")
        return

    print(f"[{ts}] Đang quét {len(pages)} Page: {', '.join(p['name'] for p in pages)}")

    changed = False
    for page in pages:
        page_id = page["id"]
        page_name = page["name"]
        page_token = page["access_token"]

        post_ids = get_recent_post_ids(page_id, page_token)
        for post_id in post_ids:
            key = f"{page_id}_{post_id}"
            # Chỉ bỏ qua hoàn toàn khi bài đã được báo CẢ 2 loại (ngưỡng chính + spike)
            # -> không còn gì để theo dõi thêm nữa. Nếu mới chỉ báo 1 trong 2 loại,
            # vẫn tiếp tục lấy dữ liệu để kiểm tra loại còn lại.
            if key in already_notified and key in already_spike_notified:
                continue

            views, comments, link, message = get_post_stats(post_id, page_token)

            if views is None or comments is None:
                print(f"[{ts}] [{page_name}] {post_id}: không lấy được dữ liệu, bỏ qua.")
                continue

            print(f"[{ts}] [{page_name}] {post_id} -> views={views} | comments={comments}")

            # --- Kiểm tra dấu hiệu "dựng đứng" (tăng đột biến) ---
            now_dt = datetime.now()
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
                continue

            # Đạt ngưỡng -> kiểm tra xem đã gắn link chưa trước khi báo
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
    save_view_history(view_history)  # luôn lưu để không mất dữ liệu lịch sử tính spike


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
