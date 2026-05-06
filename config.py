import os


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = int(os.getenv("CHAT_ID", "627161212"))
BASE_URL = os.getenv("BASE_URL", "https://sibbp.ru/")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Optional settings:
# ADMIN_IDS=627161212
# EDITOR_IDS=111111111,222222222
# DIRECTOR_IDS=333333333,444444444
# REPORT_CHAT_IDS=627161212,333333333
# DASHBOARD_URL=https://sibbp-competitor-bot-v12.onrender.com
# DASHBOARD_TOKEN=secret-key
