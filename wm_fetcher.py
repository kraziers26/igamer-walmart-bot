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

# ── Walmart endpoints ────────────────────────────────────────────────────────
WM_HOME_URL   = "https://www.walmart.com/"
WM_SEARCH_URL = "https://www.walmart.com/search"
WM_API_URL    = "https://www.walmart.com/search/api"

# Full Chrome 122 header set — Akamai checks header order and completeness
WM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":             "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "Connection":         "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":     "document",
    "Sec-Fetch-Mode":     "navigate",
    "Sec-Fetch-Site":     "none",
    "Sec-Fetch-User":     "?1",
    "Sec-CH-UA":          '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-CH-UA-Mobile":   "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "DNT":                "1",
    "Cache-Control":      "max-age=0",
}

WM_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.walmart.com/search?q=laptop",
    "Origin":          "https://www.walmart.com",
    "Sec-Fetch-Dest":  "empty",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "same-origin",
    "Sec-CH-UA":       '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-CH-UA-Mobile":   "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "DNT": "1",
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

    async def _warm_session(self, session: aiohttp.ClientSession) -> bool:
        """
        Visit Walmart homepage first to get real cookies (ak_bmsc, bm_sv, etc).
        Akamai uses these to validate subsequent API calls.
        Returns True if warm-up succeeded.
        """
        try:
            logger.info("WM: warming session via homepage...")
            async with session.get(
                WM_HOME_URL,
                headers=WM_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True,
            ) as resp:
                _ = await resp.read()   # consume body so cookies are set
                logger.info(f"WM homepage: {resp.status} | cookies: {len(session.cookie_jar)}")
                await asyncio.sleep(2)  # brief pause — mimic human timing

            # Second: hit a real search page (not API) to get search-scoped cookies
            async with session.get(
                "https://www.walmart.com/search?q=gaming+laptop",
                headers=WM_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True,
            ) as resp:
                _ = await resp.read()
                logger.info(f"WM search warmup: {resp.status}")
                await asyncio.sleep(1)

            return True
        except Exception as e:
            logger.warning(f"WM warm-up failed: {e}")
            return False

    async def fetch_all(self) -> dict:
        cache = _load_cache()
        today = datetime.utcnow().isoformat()

        connector = aiohttp.TCPConnector(ssl=True, limit=5)
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.CookieJar()) as session:
            await self._warm_session(session)

            # Sequential fetches to avoid triggering rate limits
            bsr_results, fresh_results = [], []
            for name, cat_id in CATEGORIES:
                bsr_results.append(await self._fetch_category(session, name, cat_id, sort="best_seller"))
                await asyncio.sleep(1)
                fresh_results.append(await self._fetch_category(session, name, cat_id, sort="new"))
                await asyncio.sleep(1)

        # Update price cache with today's prices
        cache_updates = []
        output = {}

        for i, (name, _) in enumerate(CATEGORIES):
            bsr_products   = bsr_results[i]   if i < len(bsr_results)   else []
            fresh_products = fresh_results[i] if i < len(fresh_results) else []

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
        Uses /search (HTML page with embedded JSON) as primary, /search/api as fallback.
        """
        logger.info(f"WM fetch: {name} | sort={sort}")

        # Try the graphql/internal API first
        params = {
            "query":            name,
            "cat_id":           cat_id,
            "sort":             sort,
            "affinityOverride": "default",
            "pref":             "true",
            "prg":              "desktop",
        }
        try:
            async with session.get(
                WM_API_URL, params=params,
                headers=WM_API_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    raw = await resp.text()
                    try:
                        data  = json.loads(raw)
                        items = (
                            data.get("items") or
                            data.get("searchResult", {}).get("itemStacks", [{}])[0].get("items", []) or
                            []
                        )
                        if items:
                            products = [p for p in items if is_new(p)]
                            logger.info(f"  {name}: {len(products)} products (sort={sort})")
                            return products[:POOL_SIZE]
                    except json.JSONDecodeError:
                        pass
                logger.warning(f"  WM API {resp.status} for {name} — trying page scrape")
        except Exception as e:
            logger.warning(f"  WM API error [{name}]: {e}")

        # Fallback: scrape the HTML search page for embedded __NEXT_DATA__ JSON
        return await self._fetch_page_scrape(session, name, cat_id, sort)

    async def _fetch_page_scrape(self, session, name: str, cat_id: str, sort: str) -> list:
        """Scrape Walmart search HTML — extracts __NEXT_DATA__ embedded JSON."""
        keyword  = CATEGORY_FALLBACKS.get(name, name)
        sort_map = {"best_seller": "best_seller", "new": "new"}
        url      = f"https://www.walmart.com/search?q={keyword.replace(' ', '+')}&sort={sort_map.get(sort, sort)}"
        logger.info(f"  WM page scrape: {url}")
        try:
            async with session.get(
                url,
                headers=WM_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"  WM page scrape {resp.status} for {name}")
                    return []
                html = await resp.text()

            # ── Method 1: __NEXT_DATA__ script tag (Next.js standard) ──────────
            items = self._extract_next_data(html, name)
            if items:
                products = [p for p in items if is_new(p)]
                logger.info(f"  WM __NEXT_DATA__ {name}: {len(products)} products")
                return products[:POOL_SIZE]

            # ── Method 2: window.__WML_REDUX_INITIAL_STATE__ ───────────────────
            items = self._extract_redux_state(html, name)
            if items:
                products = [p for p in items if is_new(p)]
                logger.info(f"  WM redux state {name}: {len(products)} products")
                return products[:POOL_SIZE]

            logger.warning(f"  WM: no products found in page for {name}")
            return []

        except Exception as e:
            logger.error(f"WM page scrape error [{name}]: {e}")
            return []

    def _extract_next_data(self, html: str, name: str) -> list:
        """Extract items from Next.js __NEXT_DATA__ script tag."""
        try:
            tag_start = html.find('<script id="__NEXT_DATA__"')
            if tag_start == -1:
                return []
            json_start = html.find(">", tag_start) + 1
            json_end   = html.find("</script>", json_start)
            blob = json.loads(html[json_start:json_end])
            return self._find_items_recursive(blob)
        except Exception as e:
            logger.debug(f"  __NEXT_DATA__ parse failed: {e}")
            return []

    def _extract_redux_state(self, html: str, name: str) -> list:
        """Extract items from WML Redux state embedded in page."""
        try:
            marker = "window.__WML_REDUX_INITIAL_STATE__"
            idx    = html.find(marker)
            if idx == -1:
                return []
            eq     = html.find("=", idx) + 1
            end    = html.find("</script>", eq)
            # Strip trailing semicolon
            raw    = html[eq:end].strip().rstrip(";")
            blob   = json.loads(raw)
            return self._find_items_recursive(blob)
        except Exception as e:
            logger.debug(f"  Redux state parse failed: {e}")
            return []

    def _find_items_recursive(self, obj, _depth=0) -> list:
        """
        Walk any JSON blob to find a list of Walmart product items.
        Checks multiple known keys Walmart uses across versions.
        """
        if _depth > 15:
            return []

        if isinstance(obj, dict):
            # Direct itemStacks pattern (most common)
            for stack_key in ("itemStacks", "item_stacks"):
                stacks = obj.get(stack_key)
                if isinstance(stacks, list):
                    for stack in stacks:
                        if isinstance(stack, dict):
                            items = stack.get("items") or stack.get("Item") or []
                            if isinstance(items, list) and items:
                                first = items[0]
                                if isinstance(first, dict) and (
                                    "itemId" in first or "usItemId" in first
                                    or "id" in first or "title" in first
                                ):
                                    return items

            # Direct items list
            for item_key in ("items", "Products", "products"):
                items = obj.get(item_key)
                if isinstance(items, list) and len(items) > 2:
                    first = items[0] if items else {}
                    if isinstance(first, dict) and (
                        "itemId" in first or "usItemId" in first
                        or "title" in first or "salePrice" in first
                        or "primaryOffer" in first
                    ):
                        return items

            # Recurse into values
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    result = self._find_items_recursive(v, _depth + 1)
                    if result:
                        return result

        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    result = self._find_items_recursive(item, _depth + 1)
                    if result:
                        return result

        return []

    async def test_connection(self) -> tuple:
        """Test Walmart API connectivity — mirrors bb_fetcher.test_connection."""
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.CookieJar()) as session:
            await self._warm_session(session)
            try:
                products = await self._fetch_category(session, "Gaming Laptops", "3944_3951_132959", sort="best_seller")
                if products:
                    sample = (products[0].get("title") or products[0].get("name") or "—")[:60]
                    return True, len(products), sample
                return False, "Connected but no products returned", ""
            except Exception as e:
                return False, f"Connection error: {e}", ""
