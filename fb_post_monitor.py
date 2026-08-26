"""
FB POST MONITOR (NHIỀU PAGE, TỰ ĐỘNG, CHẠY 24/7)
=========================================================
Tự động theo dõi TẤT CẢ (hoặc 1 phần) các Page Facebook bạn quản lý.
Với mỗi Page, script lấy danh sách bài viết MỚI NHẤT, kiểm tra views/comments,
và gửi thông báo Telegram ngay khi 1 bài đạt đủ ngưỡng — kèm tên Page + link.

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
from datetime import datetime, timezone

# ========================== CONFIG ==========================
USER_ACCESS_TOKEN = "EAAgnXcXSwJUBSbvFMzr1dtquLmkctMN2T07apqzXOc7Kitnbuf7gfuZBFk30RPvonLnHPIsLXMNxRUkqIMXiOafZB0BrYn8kAa2aZABTnxUSm0ynnQeEbplyBCHPYns6o1f9WL1QuPWPCXqND6UEqA15bFnZAbFZBES7kyfL5PlgfCI8dUfsYKQOSZAn5lVZAA3nSgmJqfXB1O9xFkl"

# Để trống [] = theo dõi TẤT CẢ Page bạn quản lý.
INCLUDE_PAGE_NAMES = []

VIEW_THRESHOLD = 5000
COMMENT_THRESHOLD = 20

# Chỉ theo dõi các bài đăng trong N giờ gần nhất (tránh quét lại bài cũ)
ONLY_POSTS_NEWER_THAN_HOURS = 72

TELEGRAM_BOT_TOKEN = "8770004220:AAEUuMts84bq8XUn6Tbyc_qYGOx0F_UZoEw"
TELEGRAM_CHAT_ID = "7513038171"

CHECK_INTERVAL_SECONDS = 60  # tần suất kiểm tra (giây)
GRAPH_API_VERSION = "v20.0"

# File lưu lại các bài đã báo/đã có link, để không báo trùng khi restart
NOTIFIED_FILE = "notified_posts.json"
# ==============================================================

GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
# post_impressions/post_impressions_unique đã bị Facebook deprecated (15/06/2026).
METRIC_FALLBACKS = ["post_media_view", "post_total_media_view_unique"]

URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)


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


already_notified = load_notified()  # tập hợp "page_id_post_id" đã xử lý xong


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
            print(f"[LỖI] Không lấy được danh sách Page: {data['error'].get('message')}")
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
        print(f"[LỖI] Page {page_id}: không lấy được danh sách bài viết: {data['error'].get('message')}")
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
        print(f"[LỖI] {post_id}: {data2['error'].get('message')}")

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
            if key in already_notified:
                continue  # đã xử lý xong bài này rồi, bỏ qua luôn không cần gọi API nữa

            views, comments, link, message = get_post_stats(post_id, page_token)

            if views is None or comments is None:
                print(f"[{ts}] [{page_name}] {post_id}: không lấy được dữ liệu, bỏ qua.")
                continue

            print(f"[{ts}] [{page_name}] {post_id} -> views={views} | comments={comments}")

            if views < VIEW_THRESHOLD or comments < COMMENT_THRESHOLD:
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


def main():
    print("Bắt đầu theo dõi bài viết trên tất cả các Page... (Ctrl+C để dừng)")
    while True:
        try:
            check_all_pages()
        except Exception as e:
            print(f"[LỖI] Lỗi không xác định trong vòng quét: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()