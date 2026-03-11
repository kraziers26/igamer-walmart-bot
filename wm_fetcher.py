"""
wm_fetcher.py — Walmart Market Intelligence Fetcher
iGamer Corp | Mirrors bb_fetcher.py structure exactly.

Data source: Walmart's internal search API (no key required).
Freshness:   SQLite price cache — compares today vs yesterday to detect drops.
             priceUpdateDate does not exist on Walmart, so we build our own.
"""

import aiohttp
import asyncio
import logging
import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ── Walmart internal search endpoint ─────────────────────────────────────────
WM_SEARCH_URL = "https://www.walmart.com/search/api"

# Realistic browser headers — required or Walmart returns 403/empty
WM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.walmart.com/",
    "Origin":          "https://www.walmart.com",
}

# ── Categories ────────────────────────────────────────────────────────────────
# cat_id = Walmart browse node IDs for each category
CATEGORIES = [
    ("Gaming Desktops", "3944_3951_443912"),
    ("Gaming Laptops",  "3944_3951_132959"),
    ("MacBooks",        "3944_3951_3951"),
    ("All-in-One PCs",  "3944_3951_7505741"),
    ("Windows Laptops", "3944_3951_1230463"),
]

# Fallback keyword searches if category ID returns empty
CATEGORY_FALLBACKS = {
    "Gaming Desktops": "gaming desktop computer",
    "Gaming Laptops":  "gaming laptop",
    "MacBooks":        "apple macbook",
    "All-in-One PCs":  "all in one desktop computer",
    "Windows Laptops": "windows laptop",
}

POOL_SIZE    = 50
DISPLAY_SIZE = 10

EXCLUDE_WORDS = ("refurbished", "open-box", "open box", "pre-owned", "preowned", "renewed", "certified used")

# ── Price cache (SQLite) ───────────────────────────────────────────────────────
DB_PATH = os.environ.get("PRICE_DB_PATH", "/data/wm_price_cache.db")

def _init_db():
    """Create price cache table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            item_id     TEXT PRIMARY KEY,
            price       REAL,
            was_price   REAL,
            last_seen   TEXT,
            drop_date   TEXT
        )
    """)
    conn.commit()
    conn.close()

def _load_cache() -> dict:
    """Load all cached prices into memory as {item_id: row_dict}."""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT item_id, price, was_price, last_seen, drop_date FROM price_history").fetchall()
        conn.close()
        return {
            r[0]: {"price": r[1], "was_price": r[2], "last_seen": r[3], "drop_date": r[4]}
            for r in rows
        }
    except Exception as e:
        logger.warning(f"Cache load failed: {e}")
        return {}

def _save_cache(updates: list):
    """
    Upsert price records. updates = list of (item_id, price, was_price, last_seen, drop_date).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany("""
            INSERT INTO price_history (item_id, price, was_price, last_seen, drop_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                price     = excluded.price,
                was_price = excluded.was_price,
                last_seen = excluded.last_seen,
                drop_date = CASE
                    WHEN excluded.price < price_history.price THEN excluded.drop_date
                    ELSE price_history.drop_date
                END
        """, updates)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Cache save failed: {e}")


# ── Product helpers ───────────────────────────────────────────────────────────

def is_new(p: dict) -> bool:
    title = (p.get("title") or p.get("name") or "").lower()
    return not any(w in title for w in EXCLUDE_WORDS)


def _extract_price(p: dict) -> tuple[float, float]:
    """
    Returns (sale_price, was_price) from Walmart product dict.
    Walmart uses different field names depending on the endpoint/sort.
    """
    # Primary offer block
    primary = p.get("primaryOffer") or p.get("primary_offer") or {}
    sale_price = float(
        primary.get("offerPrice") or
        p.get("salePrice") or
        p.get("price") or
        0
    )
    was_price = float(
        primary.get("listPrice") or
        p.get("wasPrice") or
        p.get("was_price") or
        p.get("listPrice") or
        0
    )
    return sale_price, was_price


def fresh_deal_score(p: dict, cache: dict) -> int:
    """
    Walmart Fresh Deal Score (0-13 pts) — mirrors bb_fetcher.py logic.
    Freshness derived from SQLite price cache since Walmart has no priceUpdateDate.
    """
    score     = 0
    item_id   = str(p.get("itemId") or p.get("usItemId") or "")
    cached    = cache.get(item_id, {})
    drop_date = cached.get("drop_date")

    # Freshness — days since we first detected this price drop
    if drop_date:
        try:
            dt   = datetime.fromisoformat(drop_date)
            now  = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
            days = (now - dt).days
            if days == 0:    score += 4   # dropped today
            elif days <= 2:  score += 3   # dropped in last 2 days
            elif days <= 7:  score += 1   # still relatively fresh
        except Exception:
            pass

    sale_price, was_price = _extract_price(p)

    # On sale — either Walmart flags it or we detect via was_price
    on_sale = bool(
        p.get("onSale") or
        p.get("specialBuy") or
        (was_price > 0 and sale_price < was_price)
    )
    if on_sale:
        score += 2

    # Discount depth
    if was_price > 0 and sale_price > 0:
        pct = ((was_price - sale_price) / was_price) * 100
        if pct >= 20:   score += 3
        elif pct >= 10: score += 2
        elif pct >= 5:  score += 1

        save_d = was_price - sale_price
        if save_d >= 300:   score += 2
        elif save_d >= 100: score += 1

    # Rollback badge = extra confirmation of a real price drop
    offer_type = (p.get("specialOfferType") or "").lower()
    if "rollback" in offer_type or "clearance" in offer_type:
        score += 1

    return min(score, 13)


def deal_freshness_label(p: dict, cache: dict) -> str:
    """Human-readable freshness label — mirrors bb_fetcher.py."""
    item_id   = str(p.get("itemId") or p.get("usItemId") or "")
    cached    = cache.get(item_id, {})
    drop_date = cached.get("drop_date")

    if not drop_date:
        # No history yet — first time seeing this product
        sale_price, was_price = _extract_price(p)
        if was_price > 0 and sale_price < was_price:
            return "🟢 New (1st seen)"
        return "—"
    try:
        dt   = datetime.fromisoformat(drop_date)
        now  = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
        days = (now - dt).days
        if days == 0:   return "🟢 New today"
        if days <= 2:   return f"🟢 {days}d new"
        if days <= 7:   return f"🟡 {days}d active"
        if days <= 14:  return f"🟠 {days}d aging"
        return               f"🔴 {days}d old"
    except Exception:
        return "—"


def _annotate(p: dict, cache: dict) -> dict:
    """Annotate a product dict with all scoring fields — mirrors bb_fetcher.py."""
    sale_price, was_price = _extract_price(p)
    on_sale = bool(
        p.get("onSale") or
        p.get("specialBuy") or
        (was_price > 0 and sale_price < was_price)
    )
    save_d   = max(was_price - sale_price, 0) if was_price > 0 else 0
    save_pct = (save_d / was_price * 100) if was_price > 0 else 0

    # Best seller rank — Walmart returns it as an integer in some responses
    bs_rank = p.get("bestMarketplaceRank") or p.get("bestSellerRank") or None

    sc = fresh_deal_score(p, cache)

    p["salePrice"]      = sale_price
    p["regularPrice"]   = was_price if was_price > 0 else sale_price
    p["dollarSavings"]  = round(save_d, 2)
    p["percentSavings"] = round(save_pct, 1)
    p["onSale"]         = on_sale
    p["onlineAvailability"] = p.get("availabilityStatus", "").upper() in ("IN_STOCK", "AVAILABLE", "") or \
                              p.get("available", True)
    p["manufacturer"]   = p.get("brand") or p.get("manufacturer") or "—"
    p["name"]           = p.get("title") or p.get("name") or "—"
    p["url"]            = f"https://www.walmart.com/ip/{p.get('itemId') or ''}"
    p["bestSellingRank"]  = bs_rank
    p["best_seller_rank"] = bs_rank
    p["best_seller_str"]  = f"🛒 #{bs_rank}" if bs_rank else "—"
    p["fresh_score"]      = sc
    p["freshness_label"]  = deal_freshness_label(p, cache)
    p["trending_rank"]    = None
    p["most_viewed_rank"] = None
    p["trending_str"]     = "—"
    p["most_viewed_str"]  = "—"
    # priceUpdateDate stub — report_builder.py reads this field.
    # We set it to drop_date from cache, or None if no history yet.
    item_id = str(p.get("itemId") or p.get("usItemId") or "")
    p["priceUpdateDate"] = (cache.get(item_id) or {}).get("drop_date")

    return p


# ── Fetcher class ─────────────────────────────────────────────────────────────

class WMFetcher:

    def __init__(self):
        _init_db()

    async def fetch_all(self) -> dict:
        cache = _load_cache()
        today = datetime.utcnow().isoformat()

        async with aiohttp.ClientSession(headers=WM_HEADERS) as session:
            bsr_tasks   = [self._fetch_category(session, name, cat_id, sort="best_seller") for name, cat_id in CATEGORIES]
            fresh_tasks = [self._fetch_category(session, name, cat_id, sort="new")         for name, cat_id in CATEGORIES]

            bsr_results   = await asyncio.gather(*bsr_tasks,   return_exceptions=True)
            fresh_results = await asyncio.gather(*fresh_tasks, return_exceptions=True)

        # Update price cache with today's prices
        cache_updates = []
        output = {}

        for i, (name, _) in enumerate(CATEGORIES):
            bsr_products   = bsr_results[i]   if not isinstance(bsr_results[i],   Exception) else []
            fresh_products = fresh_results[i] if not isinstance(fresh_results[i], Exception) else []

            if isinstance(bsr_results[i],   Exception): logger.error(f"BSR fetch failed [{name}]: {bsr_results[i]}")
            if isinstance(fresh_results[i], Exception): logger.error(f"Fresh fetch failed [{name}]: {fresh_results[i]}")

            all_products = {str(p.get("itemId") or p.get("usItemId") or ""): p
                            for p in bsr_products + fresh_products}.values()

            for p in all_products:
                item_id = str(p.get("itemId") or p.get("usItemId") or "")
                if not item_id:
                    continue
                sale_price, was_price = _extract_price(p)
                prev = cache.get(item_id)

                # Detect a new price drop
                if prev and sale_price > 0 and sale_price < prev["price"]:
                    drop_date = today          # price just dropped
                elif prev:
                    drop_date = prev.get("drop_date")   # keep existing drop date
                else:
                    drop_date = today if (was_price > 0 and sale_price < was_price) else None

                cache_updates.append((item_id, sale_price, was_price, today, drop_date))
                cache[item_id] = {
                    "price": sale_price, "was_price": was_price,
                    "last_seen": today,  "drop_date": drop_date
                }

            _save_cache(cache_updates)
            cache_updates = []

            # Re-fetch with updated cache and annotate
            bsr_ann   = [_annotate(p, cache) for p in bsr_products   if is_new(p)]
            fresh_ann = [_annotate(p, cache) for p in fresh_products if is_new(p)]

            # Sort fresh pool by fresh_score desc
            fresh_ann.sort(key=lambda p: p["fresh_score"], reverse=True)

            output[name] = {
                "products":       bsr_ann[:DISPLAY_SIZE],
                "pool":           bsr_ann,
                "fresh_products": fresh_ann,
            }

        return output

    async def _fetch_category(self, session, name: str, cat_id: str, sort: str = "best_seller") -> list:
        """
        Fetch products from Walmart's internal search API.
        sort: "best_seller" for established pool, "new" for freshness pass.
        """
        params = {
            "query":            name,
            "cat_id":           cat_id,
            "sort":             sort,
            "affinityOverride": "default",
            "pref":             "true",
            "prg":              "desktop",
        }
        logger.info(f"WM fetch: {name} | sort={sort}")
        try:
            async with session.get(
                WM_SEARCH_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=25)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"  WM {resp.status} for {name} — trying fallback keyword")
                    return await self._fetch_fallback(session, name, sort)
                raw = await resp.text()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"  WM non-JSON response for {name} — fallback")
                    return await self._fetch_fallback(session, name, sort)

            # Walmart wraps items in different shapes depending on endpoint
            items = (
                data.get("items") or
                data.get("searchResult", {}).get("itemStacks", [{}])[0].get("items", []) or
                []
            )
            products = [p for p in items if is_new(p)]
            logger.info(f"  {name}: {len(products)} products (sort={sort})")
            return products[:POOL_SIZE]

        except asyncio.TimeoutError:
            logger.error(f"WM timeout [{name}]")
            return []
        except Exception as e:
            logger.error(f"WM fetch error [{name}]: {e}")
            return []

    async def _fetch_fallback(self, session, name: str, sort: str) -> list:
        """Fallback: search by keyword instead of category ID."""
        keyword = CATEGORY_FALLBACKS.get(name, name)
        params  = {
            "query": keyword,
            "sort":  sort,
            "affinityOverride": "default",
        }
        logger.info(f"  WM fallback keyword search: '{keyword}'")
        try:
            async with session.get(
                WM_SEARCH_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    return []
                data  = await resp.json(content_type=None)
                items = (
                    data.get("items") or
                    data.get("searchResult", {}).get("itemStacks", [{}])[0].get("items", []) or
                    []
                )
                products = [p for p in items if is_new(p)]
                logger.info(f"  WM fallback: {len(products)} products")
                return products[:POOL_SIZE]
        except Exception as e:
            logger.error(f"WM fallback error [{name}]: {e}")
            return []

    async def test_connection(self) -> tuple:
        """Test Walmart API connectivity — mirrors bb_fetcher.test_connection."""
        async with aiohttp.ClientSession(headers=WM_HEADERS) as session:
            params = {
                "query": "gaming laptop",
                "sort":  "best_seller",
                "affinityOverride": "default",
            }
            try:
                async with session.get(
                    WM_SEARCH_URL, params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        return False, f"HTTP {resp.status} — Walmart may be blocking", ""
                    data  = await resp.json(content_type=None)
                    items = (
                        data.get("items") or
                        data.get("searchResult", {}).get("itemStacks", [{}])[0].get("items", []) or
                        []
                    )
                    if items:
                        sample = (items[0].get("title") or items[0].get("name") or "—")[:60]
                        return True, len(items), sample
                    return False, "Connected but no products returned", ""
            except Exception as e:
                return False, f"Connection error: {e}", ""
