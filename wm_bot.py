"""
wm_bot.py — iGamer Walmart Market Intelligence Bot
Standalone bot — completely separate from bb bot.py.
Same command structure, same filter system, Walmart data source.
"""

import os
import logging
from datetime import time as dtime
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

from wm_fetcher import WMFetcher
from wm_report_builder import build_report

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN")
REPORT_CHAT_ID  = os.environ.get("REPORT_CHAT_ID")
ADMIN_IDS       = set(
    int(x.strip()) for x in os.environ.get("ADMIN_TELEGRAM_ID", "0").split(",")
    if x.strip().lstrip("-").isdigit()
)
REPORT_HOUR_EST = int(os.environ.get("REPORT_HOUR_EST", "8"))

EST     = pytz.timezone("US/Eastern")
fetcher = WMFetcher()

FILTERS = {
    "trending": ("🆕 Fresh Deals",       "Newest price drops — detected via daily price cache comparison"),
    "full":     ("📦 Established Deals", "Top products by Walmart sales rank — known sellers with stable pricing"),
    "on_sale":  ("💰 On Sale Only",      "Only products currently discounted — sorted by best % off"),
    "selling":  ("🛒 Best Sellers",      "Products sorted by Walmart sales rank"),
    "hot":      ("🔴 HOT BUYS Only",    "Fresh price drop + deep discount — highest conviction buys"),
}


# ── Core report sender ────────────────────────────────────────────────────────

async def send_report(app, chat_id, filter_key="full", triggered_by="scheduled"):
    label, description = FILTERS.get(filter_key, FILTERS["full"])
    source = "📅 Scheduled" if triggered_by == "scheduled" else "⚡ On-Demand"
    try:
        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📊 *iGamer Walmart Report — {label}*\n\n"
                f"_{description}_\n\n"
                f"⏳ Pulling live Walmart data... give me 30 seconds."
            ),
            parse_mode="Markdown"
        )
        data = await fetcher.fetch_all()
        path = build_report(data, filter_key=filter_key)

        filter_notes = {
            "full":     "• All categories — full Walmart market snapshot",
            "trending": "• Filtered to products with detected price drops\n• Sorted by Fresh Deal Score",
            "selling":  "• Top selling products by Walmart sales rank\n• Sorted by rank",
            "on_sale":  "• On-sale products only\n• Sorted by best % discount",
            "hot":      "• 🔴 HOT BUYS only — price drop + deep discount",
        }
        caption = (
            f"{source} | *iGamer Corp — Walmart Market Intelligence*\n\n"
            f"*Filter: {label}*\n"
            f"{filter_notes.get(filter_key, '')}\n\n"
            f"_Tap any 🛒 Buy Now link to purchase directly on Walmart_"
        )
        with open(path, "rb") as f:
            await app.bot.send_document(
                chat_id=chat_id, document=f,
                filename=os.path.basename(path),
                caption=caption, parse_mode="Markdown"
            )
        try:
            os.remove(path)
        except Exception:
            pass
        logger.info(f"WM Report [{filter_key}] delivered to {chat_id}")

    except Exception as e:
        logger.error(f"WM Report failed [{filter_key}]: {e}")
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"❌ *Walmart report failed:* `{str(e)}`\nCheck Railway logs.",
            parse_mode="Markdown"
        )


# ── Filter keyboard ───────────────────────────────────────────────────────────

def filter_keyboard(prefix):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🆕 Fresh Deals",      callback_data=f"{prefix}_trending"),
            InlineKeyboardButton("🔴 HOT BUYS Only",    callback_data=f"{prefix}_hot"),
        ],
        [
            InlineKeyboardButton("💰 On Sale Only",     callback_data=f"{prefix}_on_sale"),
            InlineKeyboardButton("🛒 Best Sellers",     callback_data=f"{prefix}_selling"),
        ],
        [
            InlineKeyboardButton("📦 Established Deals", callback_data=f"{prefix}_full"),
        ],
        [InlineKeyboardButton("❌ Cancel",              callback_data=f"{prefix}_cancel")],
    ])


# ── /report ───────────────────────────────────────────────────────────────────

async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text(
        "📊 *Pull a Walmart Report — Choose Filter*\n\nWhat type of deals do you want?",
        reply_markup=filter_keyboard("rep"),
        parse_mode="Markdown"
    )


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ADMIN_IDS:
        await query.answer("⛔ Admin only.", show_alert=True)
        return

    filter_key = query.data.replace("rep_", "")
    if filter_key == "cancel":
        await query.edit_message_text("❌ Cancelled.")
        return

    label, description = FILTERS.get(filter_key, FILTERS["full"])
    await query.edit_message_text(
        f"✅ *{label}*\n_{description}_\n\n⏳ Building your Walmart report...",
        parse_mode="Markdown"
    )
    chat_id = REPORT_CHAT_ID or update.effective_chat.id
    await send_report(context.application, int(chat_id), filter_key=filter_key, triggered_by="on_demand")


# ── /setschedule ──────────────────────────────────────────────────────────────

async def setschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Admin only.")
        return
    current_filter = context.bot_data.get("scheduled_filter", "full")
    current_hour   = context.bot_data.get("scheduled_hour", REPORT_HOUR_EST)
    current_label  = FILTERS.get(current_filter, FILTERS["full"])[0]
    display_hour   = f"{current_hour}:00 AM" if current_hour < 12 else ("12:00 PM" if current_hour == 12 else f"{current_hour-12}:00 PM")
    await update.message.reply_text(
        f"⚙️ *Configure Walmart Scheduled Report*\n\n"
        f"Current: *{display_hour} EST* — *{current_label}*\n\n"
        f"*Step 1 — Choose filter for the scheduled report:*",
        reply_markup=filter_keyboard("sch"),
        parse_mode="Markdown"
    )


async def setschedule_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ADMIN_IDS:
        await query.answer("⛔ Admin only.", show_alert=True)
        return

    filter_key = query.data.replace("sch_", "")
    if filter_key == "cancel":
        await query.edit_message_text("❌ Cancelled.")
        return

    context.bot_data["scheduled_filter"] = filter_key
    label, _ = FILTERS.get(filter_key, FILTERS["full"])

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("6 AM",  callback_data="schtime_6"),
            InlineKeyboardButton("7 AM",  callback_data="schtime_7"),
            InlineKeyboardButton("8 AM",  callback_data="schtime_8"),
            InlineKeyboardButton("9 AM",  callback_data="schtime_9"),
        ],
        [
            InlineKeyboardButton("10 AM", callback_data="schtime_10"),
            InlineKeyboardButton("11 AM", callback_data="schtime_11"),
            InlineKeyboardButton("12 PM", callback_data="schtime_12"),
            InlineKeyboardButton("1 PM",  callback_data="schtime_13"),
        ],
    ])
    await query.edit_message_text(
        f"✅ Filter set: *{label}*\n\n*Step 2 — What time should this send every day? (EST)*",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )


async def setschedule_time_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id not in ADMIN_IDS:
        await query.answer("⛔ Admin only.", show_alert=True)
        return

    hour       = int(query.data.replace("schtime_", ""))
    filter_key = context.bot_data.get("scheduled_filter", "full")
    label, _   = FILTERS.get(filter_key, FILTERS["full"])

    for job in context.job_queue.get_jobs_by_name("wm_daily_report"):
        job.schedule_removal()

    context.job_queue.run_daily(
        scheduled_report,
        time=dtime(hour=hour, minute=0, tzinfo=EST),
        name="wm_daily_report",
        data={"filter_key": filter_key}
    )
    context.bot_data["scheduled_hour"] = hour
    display_hour = f"{hour}:00 AM" if hour < 12 else ("12:00 PM" if hour == 12 else f"{hour-12}:00 PM")

    await query.edit_message_text(
        f"✅ *Walmart Schedule Updated!*\n\n"
        f"⏰ Time: *{display_hour} EST* (daily)\n"
        f"📊 Filter: *{label}*\n\n"
        f"⚠️ To survive a Railway restart update `REPORT_HOUR_EST={hour}` in env vars.",
        parse_mode="Markdown"
    )


# ── Scheduled job ─────────────────────────────────────────────────────────────

async def scheduled_report(context: ContextTypes.DEFAULT_TYPE):
    chat_id = REPORT_CHAT_ID or next(iter(ADMIN_IDS), 0)
    if not chat_id:
        logger.error("No REPORT_CHAT_ID set for Walmart bot")
        return
    filter_key = "trending"
    if context.job and context.job.data:
        filter_key = context.job.data.get("filter_key", "trending")
    else:
        filter_key = context.bot_data.get("scheduled_filter", "trending")
    logger.info(f"WM Scheduled report → chat {chat_id} | filter: {filter_key}")
    await send_report(context.application, int(chat_id), filter_key=filter_key, triggered_by="scheduled")


# ── /schedule ─────────────────────────────────────────────────────────────────

async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Admin only.")
        return
    chat_id      = REPORT_CHAT_ID or context.bot_data.get("override_chat_id") or next(iter(ADMIN_IDS), 0)
    filter_key   = context.bot_data.get("scheduled_filter", "trending")
    hour         = context.bot_data.get("scheduled_hour", REPORT_HOUR_EST)
    label, _     = FILTERS.get(filter_key, FILTERS["full"])
    display_hour = f"{hour}:00 AM" if hour < 12 else ("12:00 PM" if hour == 12 else f"{hour-12}:00 PM")
    jobs         = context.job_queue.get_jobs_by_name("wm_daily_report")
    job_line     = f"Next run: {jobs[0].next_t}" if jobs else "No job active"
    await update.message.reply_text(
        f"⏰ *Walmart Schedule Status*\n\n"
        f"Time: *{display_hour} EST*\n"
        f"Filter: *{label}*\n"
        f"Destination: `{chat_id}`\n"
        f"{job_line}\n\n"
        f"Use /setschedule to change.",
        parse_mode="Markdown"
    )


# ── /start ────────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_admin = update.effective_user.id in ADMIN_IDS
    admin_block = (
        "\n\n*Admin commands:*\n"
        "/report — Pull a report now (choose filter)\n"
        "/setschedule — Set daily report time + filter\n"
        "/schedule — View current schedule\n"
        "/setchat — Set this chat as report destination\n"
        "/test — Quick Walmart connection test"
    ) if is_admin else ""
    await update.message.reply_text(
        "👋 *iGamer Walmart Market Report Bot*\n\n"
        "Daily Walmart market intelligence for your sales team.\n\n"
        "Report filters:\n"
        "🆕 Fresh Deals — detected price drops, scored by freshness\n"
        "🔴 HOT BUYS — fresh drop + deep discount\n"
        "🛒 Best Sellers — top by Walmart sales rank\n"
        "💰 On Sale Only — discounted, best % first\n"
        "📦 Established Deals — full snapshot"
        + admin_block,
        parse_mode="Markdown"
    )


# ── /setchat ─────────────────────────────────────────────────────────────────

async def setchat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Admin only.")
        return
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or "this chat"
    context.bot_data["override_chat_id"] = chat_id
    await update.message.reply_text(
        f"✅ *Walmart report destination set!*\n\nChat: *{chat_title}*\nID: `{chat_id}`\n\n"
        f"⚠️ Make permanent: set `REPORT_CHAT_ID={chat_id}` in Railway.",
        parse_mode="Markdown"
    )


# ── /test ─────────────────────────────────────────────────────────────────────

async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text("🔌 Testing Walmart connection...")
    try:
        ok, count, sample = await fetcher.test_connection()
        if ok:
            await update.message.reply_text(
                f"✅ *Walmart — Connected*\n\nReturned {count} products\nSample: _{sample}_",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"❌ Walmart test failed: {count}")
    except Exception as e:
        await update.message.reply_text(f"❌ Exception: `{e}`", parse_mode="Markdown")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.job_queue.run_daily(
        scheduled_report,
        time=dtime(hour=REPORT_HOUR_EST, minute=0, tzinfo=EST),
        name="wm_daily_report",
        data={"filter_key": "trending"}
    )
    logger.info(f"WM daily report scheduled at {REPORT_HOUR_EST}:00 EST — filter: Fresh Deals")

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("report",      report_cmd))
    app.add_handler(CommandHandler("schedule",    schedule_cmd))
    app.add_handler(CommandHandler("setschedule", setschedule_cmd))
    app.add_handler(CommandHandler("setchat",     setchat_cmd))
    app.add_handler(CommandHandler("test",        test_cmd))

    app.add_handler(CallbackQueryHandler(report_callback,             pattern="^rep_"))
    app.add_handler(CallbackQueryHandler(setschedule_filter_callback, pattern="^sch_"))
    app.add_handler(CallbackQueryHandler(setschedule_time_callback,   pattern="^schtime_"))

    logger.info("iGamer Walmart Market Report bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
