import os
import threading
from http.server import ThreadingHTTPServer

from aiogram import executor

from access_control import apply_access_control, send_myid
from bot_app import FLOW, dp
from dashboard_site import DashboardHandler
from extra_features import setup_extra_features
from scheduler_jobs import on_shutdown_scheduler, on_startup_scheduler


def run_dashboard_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    server.serve_forever()


# Extra analytics, Excel template/import and revision workflow.
setup_extra_features(dp, FLOW)

# Public helper command for getting Telegram ID.
dp.register_message_handler(send_myid, commands=["myid"])

# Apply role checks after all handlers are registered.
apply_access_control(dp, FLOW)


if __name__ == "__main__":
    threading.Thread(target=run_dashboard_server, daemon=True).start()

    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup_scheduler,
        on_shutdown=on_shutdown_scheduler,
    )
