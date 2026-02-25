"""
Bleems.com full scraper
-----------------------
Scrapes all shops (grouped by type: Flowers, Confections, Gifts, …),
their items, and their reviews, then uploads partitioned CSVs and images to S3.

S3 structure:
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/shops.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/items.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/reviews.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/images/{shop-name}/logo/logo.jpg
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/images/{shop-name}/products/{product-id}.jpg
  <bucket>/bleems-data/year=2026/month=02/day=23/confections/...
  ...

Environment variables (set via GitHub Actions secrets):
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION   (default: us-east-1)
  S3_BUCKET_NAME       (actual bucket name)
"""

import os
import re
import json
import time
import logging
from io import StringIO, BytesIO
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL   = "https://www.bleems.com"
COUNTRY    = "kw"
S3_BUCKET  = os.environ.get("S3_BUCKET_NAME")        # actual bucket name from secret
S3_FOLDER  = "bleems-data"                            # top-level folder inside the bucket
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
_now       = datetime.now(timezone.utc)
TODAY      = _now.strftime("%Y-%m-%d")
S3_YEAR    = _now.strftime("%Y")
S3_MONTH   = _now.strftime("%m")
S3_DAY     = _now.strftime("%d")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.bleems.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.5


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _get(url: str, **kwargs) -> requests.Response:
    """GET with retry (up to 3 times)."""
    for attempt in range(1, 4):
        try:
            resp = SESSION.get(url, timeout=30, **kwargs)
            resp.encoding = 'utf-8'  # Ensure Arabic text is decoded correctly
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            log.warning(f"Attempt {attempt}/3 failed for {url}: {exc}")
            if attempt < 3:
                time.sleep(3 * attempt)
    raise RuntimeError(f"All retries exhausted for {url}")


def _width_to_stars(style: str) -> float | None:
    """Convert CSS width% to a 1–5 star rating (20 % = 1 star)."""
    m = re.search(r"width\s*:\s*(\d+(?:\.\d+)?)%", style)
    if m:
        return round(float(m.group(1)) / 20, 1)
    return None


def _parse_reviewer(raw: str):
    """
    Parse strings like:
      'Fatma L on 11/12/2025'  → ('Fatma L',    '11/12/2025')
      '17/11/2024'             → ('',            '17/11/2024')
    """
    on_match   = re.match(r"^(.+?)\s+on\s+(\d{2}/\d{2}/\d{4})$", raw)
    date_match = re.match(r"^(\d{2}/\d{2}/\d{4})$", raw)
    if on_match:
        return on_match.group(1).strip(), on_match.group(2).strip()
    if date_match:
        return "", date_match.group(1)
    return raw.strip(), ""


# ──────────────────────────────────────────────────────────────────────────────
# Shop list
# ──────────────────────────────────────────────────────────────────────────────
def fetch_all_shops() -> list[dict]:
    """
    Parse https://www.bleems.com/kw/shops and return a list of shop dicts.
    Each shop dict contains: name, type, rating, ratings_count, slug, url, logo_url.
    """
    url = f"{BASE_URL}/{COUNTRY}/shops"
    log.info(f"Fetching shop list from {url}")
    html = _get(url).text
    soup = BeautifulSoup(html, "html.parser")

    def _parse_shops(use_data_attr: bool) -> list[dict]:
        result = []
        for el in soup.select("a.brand-a-z-list-item"):
            href     = el.get("href", "")
            slug     = href.split("/shop/")[-1].rstrip("/") if "/shop/" in href else ""
            img      = el.select_one("img")
            name_div = el.select_one(".brand-a-z-item-name")
            type_div = el.select_one(".brand-a-z-item-type")

            if use_data_attr:
                type_text = el.get("data-type", "Other").strip()
            else:
                type_text = (
                    type_div.text.strip() if (type_div and type_div.text.strip())
                    else el.get("data-type", "Other").strip()
                )

            result.append({
                "name":          (name_div.text.strip() if name_div else el.get("data-name", "")).strip(),
                "type":          type_text.title(),
                "rating":        el.get("data-rating", ""),
                "ratings_count": el.get("data-count", ""),
                "slug":          slug,
                "url":           f"{BASE_URL}{href}" if href.startswith("/") else href,
                "logo_url":      img.get("src", "") if img else "",
            })
        return result

    shops = _parse_shops(use_data_attr=False)
    unique_types = sorted({s["type"] for s in shops})
    log.info(f"Found {len(shops)} shops. Types from HTML text: {unique_types}")

    # If the listing page JS-filters to one type, use data-type attribute instead
    if len(unique_types) <= 1:
        log.warning(
            "Only one type found in visible text — falling back to data-type attribute."
        )
        shops = _parse_shops(use_data_attr=True)
        unique_types = sorted({s["type"] for s in shops})
        log.info(f"Types from data-type attribute: {unique_types}")

    return shops


# ──────────────────────────────────────────────────────────────────────────────
# Items
# ──────────────────────────────────────────────────────────────────────────────

# HTML entity map for JS strings
_HTML_ENTITIES = {
    "&#x1F382;": "🎂", "&#x1F319;": "🌙", "&#x1F381;": "🎁",
    "&#x1F338;": "🌸", "&#x1F490;": "💐", "&#x1F36C;": "🍬",
    "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
}


def _js_obj_to_json(raw: str) -> str:
    """
    Convert a JavaScript object literal (single-quoted keys/values,
    decodeHTMLString() calls) to valid JSON that json.loads() can parse.
    """
    # 1. Replace decodeHTMLString('...') with a proper JSON string
    raw = re.sub(
        r"decodeHTMLString\(['\"]([^'\"]*?)['\"]\)",
        lambda m: json.dumps(m.group(1)),
        raw,
    )

    # 2. Replace HTML entities
    for ent, char in _HTML_ENTITIES.items():
        raw = raw.replace(ent, char)

    # 3. Convert single-quoted strings to double-quoted strings
    #    Walk character by character to handle nested quotes safely
    result = []
    i = 0
    in_double = False
    while i < len(raw):
        ch = raw[i]
        if ch == '"':
            in_double = not in_double
            result.append(ch)
        elif ch == "'" and not in_double:
            # Collect until matching closing single-quote
            j = i + 1
            buf = []
            while j < len(raw):
                c = raw[j]
                if c == "'":
                    break
                if c == '"':
                    buf.append('\\"')   # escape embedded double-quotes
                else:
                    buf.append(c)
                j += 1
            result.append('"')
            result.extend(buf)
            result.append('"')
            i = j + 1
            continue
        else:
            result.append(ch)
        i += 1
    raw = "".join(result)

    # 4. Remove trailing commas before } or ]
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    return raw


def _extract_track_json(html: str) -> dict | None:
    """
    Extract the single `var trackJson = { … };` from a product page and
    return it as a parsed dict, or None if not found / unparseable.
    """
    m = re.search(r"var\s+trackJson\s*=\s*(\{.*?\})\s*;", html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(_js_obj_to_json(m.group(1)))
    except Exception as exc:
        log.debug(f"trackJson parse error: {exc}")
        return None


def _collect_product_urls(shop_html: str) -> list[tuple[str, str]]:
    """
    Return a list of (product_url, div_target_key) tuples from a shop page.
    Each .dv-item-head[data-content-target] carries the relative product path.
    """
    soup = BeautifulSoup(shop_html, "html.parser")
    seen, pairs = set(), []
    for div in soup.select(".dv-item-head[data-content-target]"):
        target = div.get("data-content-target", "").strip().lstrip("/")
        if target and target not in seen:
            seen.add(target)
            pairs.append((f"{BASE_URL}/{COUNTRY}/{target}", target))
    return pairs


def _row_from_track_json(data: dict, shop: dict) -> dict:
    """Build a flat item CSV row from a parsed trackJson dict."""
    flavors = data.get("flavor", [])
    colors  = data.get("color",  [])
    return {
        "shop_name":    shop["name"],
        "shop_type":    shop["type"],
        "product_id":   data.get("content_id", ""),
        "product_name": data.get("product", "").strip(),
        "category":     data.get("category", ""),
        "brand":        data.get("brand", ""),
        "price":        data.get("product_price", ""),
        "currency":     data.get("currency", "KWD"),
        "occasion":     data.get("occasion", ""),
        "product_type": data.get("product_type", ""),
        "sub_category": data.get("sub_category", ""),
        "flavors":      ", ".join(flavors) if isinstance(flavors, list) else str(flavors),
        "colors":       ", ".join(colors)  if isinstance(colors,  list) else str(colors),
        "product_url":  data.get("product_url", ""),
        "image_url":    data.get("product_image_url", ""),
    }


def fetch_shop_items(shop_html: str, shop: dict, s3: "boto3.client") -> list[dict]:
    """
    Fetch all products for a shop by visiting each individual product page
    (trackJson is only embedded on the product detail page, not the shop listing).

    Downloads and uploads product images to S3.
    Falls back to minimal row (product_id + image only) if a page fails.
    """
    pairs    = _collect_product_urls(shop_html)
    shop_soup = BeautifulSoup(shop_html, "html.parser")

    # Build lookup: target_key → div, for fallback metadata
    div_lookup: dict[str, object] = {}
    for div in shop_soup.select(".dv-item-head[data-content-target]"):
        key = div.get("data-content-target", "").strip().lstrip("/")
        div_lookup[key] = div

    items = []
    log.info(f"    Fetching {len(pairs)} product pages …")

    # Clean shop name for folder path
    clean_shop_name = re.sub(r'[^\w\-]', '_', shop['name'])
    shop_type_folder = shop['type']  # Use same case as CSV files (Flowers, Confections)

    for prod_url, target_key in pairs:
        time.sleep(REQUEST_DELAY)
        data = None
        try:
            resp = SESSION.get(prod_url, timeout=30)
            resp.encoding = 'utf-8'  # Ensure Arabic text is decoded correctly
            if resp.status_code == 200:
                data = _extract_track_json(resp.text)
        except requests.RequestException as exc:
            log.debug(f"    Product fetch error {prod_url}: {exc}")

        if data:
            item = _row_from_track_json(data, shop)
        else:
            # Minimal fallback row from shop listing div
            div = div_lookup.get(target_key)
            pid    = div.get("data-content-name", "").replace("Product_", "") if div else ""
            img_el = div.select_one("img") if div else None
            item = {
                "shop_name":    shop["name"],
                "shop_type":    shop["type"],
                "product_id":   pid,
                "product_name": "",
                "category":     "",
                "brand":        shop["name"],
                "price":        "",
                "currency":     "KWD",
                "occasion":     "",
                "product_type": "",
                "sub_category": "",
                "flavors":      "",
                "colors":       "",
                "product_url":  prod_url,
                "image_url":    img_el.get("src", "") if img_el else "",
            }
        
        # Download and upload product image to S3
        item["s3_image_path"] = ""
        if item.get("image_url"):
            product_id = item.get("product_id", "unknown")
            # Extract file extension
            ext = "jpg"
            if "." in item["image_url"]:
                ext = item["image_url"].split(".")[-1].split("?")[0][:4]
            
            # Partition by date first: bleems-data/year=2026/month=02/day=23/Flowers/images/...
            s3_image_path = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type_folder}/images/{clean_shop_name}/products/{product_id}.{ext}"
            uploaded_path = upload_image_to_s3(item["image_url"], s3_image_path, s3)
            if uploaded_path:
                item["s3_image_path"] = uploaded_path

        items.append(item)

    return items


# ──────────────────────────────────────────────────────────────────────────────
# Reviews
# ──────────────────────────────────────────────────────────────────────────────

# Set DEBUG_HTML=1 locally to dump the first shop page HTML for inspection
DEBUG_HTML = os.environ.get("DEBUG_HTML", "0") == "1"
_debug_dumped = False


def _make_review_row(shop: dict, text: str, raw_name: str, style: str) -> dict:
    reviewer_name, review_date = _parse_reviewer(raw_name.strip())
    star_rating = _width_to_stars(style)
    return {
        "shop_name":     shop["name"],
        "shop_type":     shop["type"],
        "reviewer_name": reviewer_name,
        "review_date":   review_date,
        "review_text":   text.strip(),
        "star_rating":   star_rating,
        "scraped_date":  TODAY,
    }


def _parse_reviews_from_html(html: str, shop: dict) -> list[dict]:
    """
    Two-strategy review parser.

    Strategy A — lxml soup (handles invalid HTML like <li> inside <div>):
      Searches by class name only (no tag restriction) to survive the
      parser restructuring orphan <li> tags.

    Strategy B — regex on raw HTML:
      Falls back to scanning the raw HTML string for .dv-reviews-text /
      .dv-reviews-name / .rating-on blocks when lxml still finds nothing.
    """
    rows = []

    # ── Strategy A: lxml parser ───────────────────────────────────────────────
    # Ensure HTML is properly encoded as UTF-8 bytes for BeautifulSoup
    try:
        if isinstance(html, str):
            html_bytes = html.encode('utf-8')
        else:
            html_bytes = html
        soup = BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")
    except Exception:
        if isinstance(html, str):
            html_bytes = html.encode('utf-8')
        else:
            html_bytes = html
        soup = BeautifulSoup(html_bytes, "html.parser", from_encoding="utf-8")

    # Search by class only — works even if parser changes <li> → something else
    for el in soup.find_all(class_="li-reviews"):
        text_el   = el.find(class_="dv-reviews-text")
        name_el   = el.find(class_="dv-reviews-name")
        rating_el = el.find(class_="rating-on")
        
        rows.append(_make_review_row(
            shop,
            text_el.get_text() if text_el else "",
            name_el.get_text() if name_el else "",
            rating_el.get("style", "") if rating_el else "",
        ))

    if rows:
        return rows

    # ── Strategy B: raw-HTML regex ────────────────────────────────────────────
    # Walk through every li-reviews block in the raw HTML string
    block_pat = re.compile(
        r'class=["\']li-reviews["\'].*?(?=class=["\']li-reviews["\']|</ul|</div\s*id=["\']dv_reviews)',
        re.DOTALL,
    )
    text_pat   = re.compile(r'class=["\']dv-reviews-text["\'][^>]*>\s*(.*?)\s*</div', re.DOTALL)
    name_pat   = re.compile(r'class=["\']dv-reviews-name["\'][^>]*>\s*(.*?)\s*</div', re.DOTALL)
    rating_pat = re.compile(r'class=["\']rating-on["\']\s+style=["\']([^"\']+)["\']', re.DOTALL)

    for block in block_pat.finditer(html):
        segment = block.group(0)
        text_m   = text_pat.search(segment)
        name_m   = name_pat.search(segment)
        rating_m = rating_pat.search(segment)
        rows.append(_make_review_row(
            shop,
            re.sub(r"<[^>]+>", "", text_m.group(1))   if text_m   else "",
            re.sub(r"<[^>]+>", "", name_m.group(1))   if name_m   else "",
            rating_m.group(1)                          if rating_m else "",
        ))

    return rows


def _parse_reviews_from_soup(soup: BeautifulSoup, shop: dict) -> list[dict]:
    """Legacy wrapper kept for compatibility with AJAX response parsing."""
    rows = []
    for el in soup.find_all(class_="li-reviews"):
        text_el   = el.find(class_="dv-reviews-text")
        name_el   = el.find(class_="dv-reviews-name")
        rating_el = el.find(class_="rating-on")
        rows.append(_make_review_row(
            shop,
            text_el.get_text()  if text_el   else "",
            name_el.get_text()  if name_el   else "",
            rating_el.get("style", "") if rating_el else "",
        ))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Per-shop fetch
# ──────────────────────────────────────────────────────────────────────────────
def _get_csrf_token(html: str) -> str:
    """
    Extract ASP.NET RequestVerificationToken from a page's hidden input.
    Handles any attribute order: name/type/value can appear in any sequence.
    """
    # Find every <input ...> tag that contains __RequestVerificationToken
    for tag in re.findall(r'<input[^>]+>', html):
        if '__RequestVerificationToken' in tag:
            # Extract value="..." from this tag
            m = re.search(r'value=["\']([^"\']+)["\']', tag)
            if m:
                return m.group(1)
    return ""


def _extract_shop_link(html: str, slug: str) -> str:
    """
    Find the shopLink value the page's own JavaScript uses for the reviews API.

    The site embeds something like:
        var shopLink = '/kw/shop/89sweet';
    or it may live in a data attribute:
        data-shop-link="/kw/shop/89sweet"

    Falls back to the full path  /<COUNTRY>/shop/<slug>  if nothing is found.
    """
    # 1) JS variable: var shopLink = '...'; or shopLink = "...";
    m = re.search(r'shopLink\s*=\s*["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)

    # 2) data attribute: data-shop-link="..."
    m = re.search(r'data-shop-link=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)

    # 3) Safe fallback — full relative path
    return f"/{COUNTRY}/shop/{slug}"


def fetch_reviews_for_shop(shop_slug: str, shop: dict, page_html: str) -> list[dict]:
    """
    Load ALL reviews via the AJAX endpoint used by the site:
      GET https://www.bleems.com/kw/ItemsList?handler=LoadReviews
      Params: shopLink=<bare-slug>&pageNo=<n>&pageSize=20
      Header: RequestVerificationToken: <token>

    Real JS example:
        $.ajax({
            url: "https://www.bleems.com/kw/ItemsList?handler=LoadReviews",
            data: {'shopLink':'auntyjujus', 'pageNo':1, 'pageSize':'20'},
            beforeSend: function(xhr) {
                xhr.setRequestHeader("RequestVerificationToken", $('input[name="__RequestVerificationToken"]').val());
            }
        });

    Uses a fresh isolated session to avoid cookie pollution from product pages.
    Paginates automatically until canLoad=false.
    """
    REVIEWS_URL = f"{BASE_URL}/{COUNTRY}/ItemsList?handler=LoadReviews"
    shop_url    = f"{BASE_URL}/{COUNTRY}/shop/{shop_slug}"

    # ── Fresh isolated session — clean cookie jar, no product-page pollution ──
    rev_session = requests.Session()
    rev_session.headers.update(HEADERS)

    try:
        fresh_resp = rev_session.get(shop_url, timeout=30)
        fresh_resp.encoding = 'utf-8'  # Ensure Arabic text is decoded correctly
        fresh_html = fresh_resp.text
    except requests.RequestException as exc:
        log.warning(f"    Could not fetch shop page for CSRF: {exc}")
        return []

    csrf_token = _get_csrf_token(fresh_html)
    if not csrf_token:
        log.warning(f"    No CSRF token found for {shop['name']} – reviews skipped")
        return []

    log.info(f"    shopLink={shop_slug!r} (bare slug)  CSRF={csrf_token[:12]}…")

    get_headers = {
        "X-Requested-With":         "XMLHttpRequest",
        "RequestVerificationToken": csrf_token,  # Required by ASP.NET handler
        "Accept":                   "application/json, text/javascript, */*; q=0.01",
        "Referer":                  shop_url,
    }

    all_rows: list[dict] = []
    page_no = 1

    while True:
        params = {
            "shopLink": shop_slug,   # BARE SLUG (e.g. 'auntyjujus', not '/kw/shop/auntyjujus')
            "pageNo":   str(page_no),
            "pageSize": "20",
        }
        if page_no == 1:  # Log first request for debugging
            log.info(f"    GET {REVIEWS_URL}?shopLink={shop_slug}&pageNo=1&pageSize=20")
        try:
            resp = rev_session.get(
                REVIEWS_URL, params=params, headers=get_headers, timeout=30
            )
            
            if not resp.ok:
                log.warning(
                    f"    Reviews HTTP {resp.status_code} (page {page_no}):\n"
                    f"      Response headers: {dict(resp.headers)}\n"
                    f"      Body (first 400): {resp.content.decode('utf-8', errors='ignore')[:400].strip()}"
                )
                break
        except requests.RequestException as exc:
            log.warning(f"    Reviews request failed (page {page_no}): {exc}")
            break

        # Decode response as UTF-8 explicitly to handle Arabic text correctly
        try:
            text = resp.content.decode('utf-8')
        except UnicodeDecodeError:
            text = resp.content.decode('utf-8', errors='replace')
        
        log.info(f"    Reviews GET {page_no}: status={resp.status_code} len={len(text)}")

        try:
            j        = json.loads(text)
            fragment = j.get("html", "")
            can_load = j.get("canLoad", False)
        except (json.JSONDecodeError, ValueError):
            log.warning(f"    Reviews page {page_no} not JSON — treating as HTML fragment")
            fragment = text
            can_load = False

        rows = _parse_reviews_from_html(fragment, shop)
        all_rows.extend(rows)
        log.debug(f"    Reviews page {page_no}: {len(rows)} rows (canLoad={can_load})")

        if not can_load or not rows:
            break

        page_no += 1
        time.sleep(REQUEST_DELAY)

    return all_rows



def fetch_shop_data(shop: dict, s3: "boto3.client") -> tuple[list[dict], list[dict], dict]:
    """
    Fetch a single shop page.
    Returns (items, reviews, enriched_shop_dict).
    Downloads and uploads shop logo and product images to S3.
    """
    url = f"{BASE_URL}/{COUNTRY}/shop/{shop['slug']}"
    try:
        resp = _get(url)
    except RuntimeError as exc:
        log.error(f"Skipping {shop['name']}: {exc}")
        return [], [], shop

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # ── Refresh overall rating from page ──────────────────────────────────────
    rating_span = soup.select_one("span.spn-item-ratings")
    if rating_span:
        rating_on = rating_span.select_one(".rating-on")
        if rating_on:
            shop["rating"] = _width_to_stars(rating_on.get("style", ""))
        count_el = rating_span.select_one(".fw-bold")
        if count_el:
            m = re.search(r"\d+", count_el.text)
            if m:
                shop["ratings_count"] = int(m.group())

    # ── Download and upload shop logo to S3 ───────────────────────────────────
    shop["s3_image_path"] = ""
    if shop.get("logo_url"):
        # Clean shop name for folder path (remove special chars)
        clean_shop_name = re.sub(r'[^\w\-]', '_', shop['name'])
        shop_type_folder = shop['type']  # Use same case as CSV files (Flowers, Confections)
        
        # Extract file extension from URL
        ext = "jpg"
        if "." in shop["logo_url"]:
            ext = shop["logo_url"].split(".")[-1].split("?")[0][:4]
        
        # Partition by date first: bleems-data/year=2026/month=02/day=23/Flowers/images/...
        s3_logo_path = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type_folder}/images/{clean_shop_name}/logo/logo.{ext}"
        uploaded_path = upload_image_to_s3(shop["logo_url"], s3_logo_path, s3)
        if uploaded_path:
            shop["s3_image_path"] = uploaded_path
            log.debug(f"    Logo uploaded: {uploaded_path}")

    items = fetch_shop_items(html, shop, s3)

    # ── Reviews: try inline HTML first (two strategies), then AJAX ────────────
    global _debug_dumped
    if DEBUG_HTML and not _debug_dumped:
        dump_path = "debug_shop.html"
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"    DEBUG: raw HTML saved to {dump_path}")
        _debug_dumped = True

    reviews = _parse_reviews_from_html(html, shop)
    log.info(f"    inline reviews found: {len(reviews)}")

    if not reviews:
        shop_slug = shop.get("slug", "")
        reviews = fetch_reviews_for_shop(shop_slug, shop, html)
        if not reviews:
            log.warning(f"    0 reviews for {shop['name']}")
        else:
            log.info(f"    reviews via AJAX: {len(reviews)}")

    shop["scraped_date"] = TODAY
    return items, reviews, shop


# ──────────────────────────────────────────────────────────────────────────────
# S3 upload
# ──────────────────────────────────────────────────────────────────────────────
def upload_image_to_s3(image_url: str, s3_path: str, s3: "boto3.client") -> str:
    """
    Download an image from a URL and upload it to S3.
    Returns the S3 path on success, empty string on failure.
    """
    if not image_url or not s3_path:
        return ""
    
    try:
        # Download image
        resp = SESSION.get(image_url, timeout=30, stream=True)
        resp.raise_for_status()
        
        # Determine content type
        content_type = resp.headers.get('Content-Type', 'image/jpeg')
        
        # Upload to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=resp.content,
            ContentType=content_type,
        )
        return s3_path
    except Exception as exc:
        log.debug(f"    Image upload failed {image_url} -> {s3_path}: {exc}")
        return ""


def upload_df_to_s3(df: pd.DataFrame, s3: "boto3.client", key: str):
    """Serialize a DataFrame as UTF-8 CSV with BOM and put it in S3."""
    # Write to StringIO first, then encode to bytes with BOM
    from io import StringIO
    buf = StringIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    # Get the string and encode with UTF-8 BOM for Excel compatibility
    csv_bytes = buf.getvalue().encode("utf-8-sig")
    try:
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = csv_bytes,
            ContentType = "text/csv; charset=utf-8",
        )
        log.info(f"✓  s3://{S3_BUCKET}/{key}  ({len(df)} rows)")
    except ClientError as exc:
        log.error(f"S3 upload failed for {key}: {exc}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Bleems.com shops")
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Specific category to scrape (e.g., 'Flowers', 'Confections'). If not provided, scrapes all categories."
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List available categories as JSON and exit (for GitHub Actions dynamic matrix)"
    )
    args = parser.parse_args()
    
    log.info(f"Run date : {TODAY}")
    log.info(f"S3 bucket: {S3_BUCKET}")
    if args.category:
        log.info(f"Target category: {args.category}")

    # ── List categories mode ──────────────────────────────────────────────────
    if args.list_categories:
        all_shops = fetch_all_shops()
        if not all_shops:
            print(json.dumps([]))
            return
        
        categories = sorted(set(shop["type"] or "Other" for shop in all_shops))
        print(json.dumps(categories))
        return

    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name           = AWS_REGION,
    )

    # ── 1. Fetch shop list ────────────────────────────────────────────────────
    all_shops = fetch_all_shops()
    if not all_shops:
        log.error("No shops found – aborting.")
        return

    # ── 2. Group by type ──────────────────────────────────────────────────────
    by_type: dict[str, list[dict]] = {}
    for shop in all_shops:
        t = shop["type"] or "Other"
        by_type.setdefault(t, []).append(shop)

    log.info(f"Types detected: {sorted(by_type.keys())}")
    
    # ── 3. Filter by category if specified ────────────────────────────────────
    if args.category:
        if args.category in by_type:
            by_type = {args.category: by_type[args.category]}
            log.info(f"Processing only category: {args.category}")
        else:
            log.error(f"Category '{args.category}' not found. Available: {sorted(by_type.keys())}")
            return

    # ── 4. Process each type ──────────────────────────────────────────────────
    for shop_type, shops in sorted(by_type.items()):
        log.info(f"\n{'─'*60}")
        log.info(f"Processing type: {shop_type}  ({len(shops)} shops)")
        log.info(f"{'─'*60}")

        all_items:   list[dict] = []
        all_reviews: list[dict] = []
        enriched:    list[dict] = []

        for idx, shop in enumerate(shops, 1):
            log.info(f"  [{idx:>3}/{len(shops)}] {shop['name']}")

            if not shop.get("slug"):
                log.warning("    No slug – skipped")
                shop["s3_image_path"] = ""  # Add empty s3_image_path for consistency
                enriched.append(shop)
                continue

            items, reviews, updated_shop = fetch_shop_data(shop, s3)
            all_items.extend(items)
            all_reviews.extend(reviews)
            enriched.append(updated_shop)

            log.info(f"         items={len(items)}  reviews={len(reviews)}")
            time.sleep(REQUEST_DELAY)

        # S3 key prefix: bleems-data/year=2026/month=02/day=21/Flowers/
        prefix = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type}"

        # Count images uploaded
        shops_with_images = sum(1 for s in enriched if s.get("s3_image_path"))
        items_with_images = sum(1 for i in all_items if i.get("s3_image_path"))
        
        upload_df_to_s3(pd.DataFrame(enriched),    s3, f"{prefix}/shops.csv")

        if all_items:
            upload_df_to_s3(pd.DataFrame(all_items),   s3, f"{prefix}/items.csv")
        else:
            log.warning(f"  No items found for {shop_type}")

        if all_reviews:
            upload_df_to_s3(pd.DataFrame(all_reviews), s3, f"{prefix}/reviews.csv")
        else:
            log.warning(f"  No reviews found for {shop_type}")

        log.info(
            f"  Done {shop_type}: {len(enriched)} shops, {len(all_items)} items, "
            f"{len(all_reviews)} reviews | Images: {shops_with_images} shop logos, "
            f"{items_with_images} product images"
        )

    log.info("\nAll done!")


if __name__ == "__main__":
    main()
