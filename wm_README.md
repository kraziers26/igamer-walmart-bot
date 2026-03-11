# iGamer Walmart Market Intelligence Bot

Standalone Walmart bot — separate from the Best Buy bot.
Same architecture, same daily Excel report, same Telegram commands.

---

## Files in This Service

| File | Purpose |
|---|---|
| `wm_bot.py` | Main bot — Telegram commands, scheduler, report sender |
| `wm_fetcher.py` | Walmart data fetcher — internal API + SQLite price cache |
| `wm_report_builder.py` | Excel report builder — Walmart labels/branding |
| `wm_requirements.txt` | Python dependencies (rename to requirements.txt on deploy) |
| `wm_Procfile` | Railway start command (rename to Procfile on deploy) |

---

## Railway Setup — New Service

This is a **separate Railway service** from the Best Buy bot.
Both services can live in the same Railway project.

### Step 1 — Create a new bot via BotFather
- Open Telegram → search `@BotFather`
- Send `/newbot`
- Name it: `iGamer Walmart Intel` (or anything you prefer)
- Copy the token — this is your `TELEGRAM_TOKEN`

### Step 2 — Create new Railway service
In your existing Railway project:
- Click **New Service → Empty Service**
- Name it `walmart-bot`
- Connect to your GitHub repo (or use Railway CLI to push)

### Step 3 — Add Persistent Volume
- In the service settings → **Volumes → Add Volume**
- Mount path: `/data`
- Size: 1 GB (costs ~$0.25/month)
- This stores the SQLite price cache (`wm_price_cache.db`)

### Step 4 — Set Environment Variables
In Railway → your walmart-bot service → Variables:

```
TELEGRAM_TOKEN      = <new walmart bot token from BotFather>
REPORT_CHAT_ID      = <your group chat ID, negative number>
ADMIN_TELEGRAM_ID   = <your Telegram user ID, same as BB bot>
REPORT_HOUR_EST     = 8
PRICE_DB_PATH       = /data/wm_price_cache.db
```

No API key needed — Walmart requires no key.

### Step 5 — Deploy files
Rename before deploying to Railway:
- `wm_requirements.txt` → `requirements.txt`
- `wm_Procfile`         → `Procfile`

Push these files to your Railway service:
```
wm_bot.py
wm_fetcher.py
wm_report_builder.py
requirements.txt
Procfile
```

---

## How the Price Cache Works

Walmart doesn't expose a `priceUpdateDate` field like Best Buy does.
Instead, the bot builds its own price history in SQLite:

- **Day 1:** All prices saved as baseline. Deal Age shows "— No history yet" for most products.
- **Day 2:** Bot compares today vs yesterday. Any price drops are flagged. Freshness scoring begins.
- **Day 3+:** Full freshness scoring active. HOT BUY detection fully reliable.

The SQLite file lives at `/data/wm_price_cache.db` on the Railway volume.
It survives restarts and redeploys automatically.

---

## Bot Commands

| Command | What it does |
|---|---|
| `/start` | Shows bot info and available filters |
| `/report` | Pull an on-demand report — pick filter from inline keyboard |
| `/setschedule` | Change daily send time and default filter |
| `/schedule` | View current schedule status |
| `/setchat` | Set the current group as report destination |
| `/test` | Test Walmart connection and return sample product |

---

## Report Filters

| Filter | What you get |
|---|---|
| 🆕 Fresh Deals | Products with detected price drops, sorted by Fresh Deal Score |
| 🔴 HOT BUYS Only | Score ≥9 — fresh drop + deep discount — highest conviction |
| 💰 On Sale Only | All discounted products, sorted by % off |
| 🛒 Best Sellers | Sorted by Walmart sales rank |
| 📦 Established Deals | Full snapshot — top 10 per category by sales rank |

Default 8am report uses: 🆕 Fresh Deals

---

## Excel Report Structure

- **📊 SUMMARY tab:** Category overview table + Today's Top Deals + Signal Key legend
- **One tab per category:** Gaming Desktops, Gaming Laptops, MacBooks, All-in-One PCs, Windows Laptops
- **Columns:** Rank, Brand, Product Name, Sale Price, Reg Price, Save $, Save %, On Sale, In Stock, WM Rank, Fresh Score, Signal, Deal Age, Buy Link
- **Row colors:** 🔴 Red = HOT BUY (score ≥9) | 🟢 Green = 15%+ off | 🟡 Yellow = 5-14% off | Grey = out of stock

---

## Cost

| Item | Cost |
|---|---|
| Railway service compute | ~$1–2/month (idle most of the day) |
| Persistent volume (1GB) | ~$0.25/month |
| Walmart data | Free |
| **Total** | **~$1.25–2.25/month** (within existing Hobby plan $5 credit) |
