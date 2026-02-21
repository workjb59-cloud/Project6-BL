"""
Bleems.com full scraper
-----------------------
Scrapes all shops (grouped by type: Flowers, Confections, Gifts, …),
their items, and their reviews, then uploads partitioned CSVs to S3.

S3 structure:
  <bucket>/Flowers/date=YYYY-MM-DD/shops.csv
  <bucket>/Flowers/date=YYYY-MM-DD/items.csv
  <bucket>/Flowers/date=YYYY-MM-DD/reviews.csv
  <bucket>/Confections/date=YYYY-MM-DD/...
  ...

Environment variables (set via GitHub Actions secrets):
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION   (default: us-east-1)
  S3_BUCKET_NAME       (default: bleems-data)
"""

import os
import re
import json
import time
import logging
from io import StringIO
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
    soup = BeautifulSoup(_get(url).text, "html.parser")

    shops = []
    for el in soup.select("a.brand-a-z-list-item"):
        href = el.get("href", "")
        slug = href.split("/shop/")[-1].rstrip("/") if "/shop/" in href else ""
        img  = el.select_one("img")

        name_div = el.select_one(".brand-a-z-item-name")
        type_div = el.select_one(".brand-a-z-item-type")

        raw_type = (
            type_div.text.strip() if type_div else el.get("data-type", "Other")
        ).title()

        shops.append(
            {
                "name":          (name_div.text.strip() if name_div else el.get("data-name", "")).strip(),
                "type":          raw_type,
                "rating":        el.get("data-rating", ""),
                "ratings_count": el.get("data-count", ""),
                "slug":          slug,
                "url":           f"{BASE_URL}{href}" if href.startswith("/") else href,
                "logo_url":      img.get("src", "") if img else "",
            }
        )

    log.info(f"Found {len(shops)} shops total")
    return shops


# ──────────────────────────────────────────────────────────────────────────────
# Items
# ──────────────────────────────────────────────────────────────────────────────
def _extract_track_json_blocks(html: str) -> list[dict]:
    """
    Pull every `var trackJson = { … };` block from a page's HTML
    and return a list of parsed dicts.
    """
    results = []
    # Match multiline JS object literal assigned to trackJson
    pattern = re.compile(
        r"var\s+trackJson\s*=\s*(\{.*?\})\s*;",
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        raw = m.group(1)
        # Replace JS function calls: decodeHTMLString('…') → "…"
        raw = re.sub(r"decodeHTMLString\(['\"]([^'\"]*?)['\"]\)", r'"\1"', raw)
        # Replace HTML entities inside strings
        raw = raw.replace("&#x1F382;", "🎂").replace("&amp;", "&")
        # Remove trailing commas before } or ]
        raw = re.sub(r",\s*([}\]])", r"\1", raw)
        try:
            results.append(json.loads(raw))
        except json.JSONDecodeError as exc:
            log.debug(f"trackJson parse error: {exc}")
    return results


def _parse_items_from_page(html: str, shop: dict) -> list[dict]:
    """Extract product rows from a shop page."""
    soup  = BeautifulSoup(html, "html.parser")
    items = []

    # ── 1. Pull structured data from embedded trackJson ──────────────────────
    for data in _extract_track_json_blocks(html):
        flavors = data.get("flavor", [])
        colors  = data.get("color",  [])
        items.append(
            {
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
        )

    # ── 2. Fallback: parse .dv-item-head elements for anything missed ─────────
    seen_ids = {i["product_id"] for i in items}
    for div in soup.select(".dv-item-head"):
        pid  = div.get("data-content-name", "").replace("Product_", "")
        a    = div.select_one("a.item-img")
        img  = div.select_one("img")
        if pid and pid not in seen_ids:
            items.append(
                {
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
                    "product_url":  (a.get("href", "") if a else ""),
                    "image_url":    (img.get("src", "") if img else ""),
                }
            )
            seen_ids.add(pid)

    return items


# ──────────────────────────────────────────────────────────────────────────────
# Reviews
# ──────────────────────────────────────────────────────────────────────────────
def _parse_reviews_from_soup(soup: BeautifulSoup, shop: dict) -> list[dict]:
    """Parse all <li class="li-reviews"> from a BeautifulSoup object."""
    rows = []
    for li in soup.select("li.li-reviews"):
        text_div = li.select_one(".dv-reviews-text")
        name_div = li.select_one(".dv-reviews-name")
        rating_holder = li.select_one(".rating-holder .rating-on")

        review_text = text_div.text.strip() if text_div else ""
        raw_name    = name_div.text.strip()  if name_div else ""

        reviewer_name, review_date = _parse_reviewer(raw_name)
        star_rating = _width_to_stars(rating_holder.get("style", "") if rating_holder else "")

        rows.append(
            {
                "shop_name":     shop["name"],
                "shop_type":     shop["type"],
                "reviewer_name": reviewer_name,
                "review_date":   review_date,
                "review_text":   review_text,
                "star_rating":   star_rating,
                "scraped_date":  TODAY,
            }
        )
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Per-shop fetch
# ──────────────────────────────────────────────────────────────────────────────
def _extract_shop_id(html: str) -> str | None:
    """
    Try to pull the numeric shop ID from the page HTML.
    Looks for:
      - data-content-piece="shopId=1077&..."
      - var shopId = 1077;
      - /GetShopReviews?shopId=1077
      - ShopId=1077 in any JS/data attribute
    """
    patterns = [
        r"shopId[=\s:\"']+(\d+)",           # shopId=1077  shopId: 1077
        r"ShopId[=\s:\"']+(\d+)",
        r"\"shop_id\"\s*:\s*(\d+)",
        r"data-shopid=[\"'](\d+)[\"']",
        r"/GetShopReviews\?shopId=(\d+)",
        r"getReviews\(\s*(\d+)",
        r"loadReviews\(\s*(\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _sniff_reviews_ajax_url(html: str) -> str | None:
    """
    Try to extract the exact AJAX URL used for reviews directly from the
    page's inline JavaScript.  Looks for patterns like:
      $.ajax({ url: '/kw/GetShopReviews', ... })
      fetch('/kw/ReviewsPartial?shopId=...')
      $.get('/kw/GetReviews', ...
      axios.get('/kw/...')
    Returns the full absolute URL or None.
    """
    patterns = [
        # jQuery $.ajax url: '...'
        r"""url\s*:\s*['"](\S*[Rr]eview\S*?)['"]""",
        # fetch(url) / $.get(url) / axios.get(url)
        r"""(?:fetch|\.get|axios\.get)\s*\(\s*['"](\S*[Rr]eview\S*?)['"]""",
        # href/action containing review
        r"""['"](\/[^\s'"]*[Rr]eview[^\s'"]*)['"']""",
    ]
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            path = m.group(1)
            if path.startswith("http"):
                return path
            return f"{BASE_URL}{path}"
    return None


def _fetch_reviews_via_api(shop_id: str, shop: dict, sniffed_url: str | None = None) -> list[dict]:
    """
    Try every known AJAX endpoint that Bleems uses to serve reviews.
    Returns a list of review dicts (may be empty if all attempts fail).
    """
    ajax_headers = {
        **HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": f"{BASE_URL}/{COUNTRY}/shop/{shop['slug']}",
    }

    candidate_urls = []
    # Put the URL sniffed from the page's own JS first (most reliable)
    if sniffed_url:
        # Inject shopId if the URL doesn't already contain it
        if shop_id and "shopId" not in sniffed_url and "shopid" not in sniffed_url.lower():
            sep = "&" if "?" in sniffed_url else "?"
            candidate_urls.append(f"{sniffed_url}{sep}shopId={shop_id}")
        else:
            candidate_urls.append(sniffed_url)

    candidate_urls += [
        # Common patterns observed on .NET MVC e-commerce sites
        f"{BASE_URL}/{COUNTRY}/GetShopReviews?shopId={shop_id}",
        f"{BASE_URL}/{COUNTRY}/Shop/GetReviews?shopId={shop_id}",
        f"{BASE_URL}/{COUNTRY}/Reviews/GetShopReviews?shopId={shop_id}",
        f"{BASE_URL}/{COUNTRY}/shop/{shop['slug']}/GetReviews",
        f"{BASE_URL}/{COUNTRY}/GetReviews?shopId={shop_id}",
        f"{BASE_URL}/api/shop/{shop_id}/reviews",
        f"{BASE_URL}/api/GetShopReviews?shopId={shop_id}",
    ]

    for url in candidate_urls:
        try:
            resp = SESSION.get(url, headers=ajax_headers, timeout=20)
            if resp.status_code != 200 or len(resp.text.strip()) < 20:
                continue

            content_type = resp.headers.get("Content-Type", "")

            # ── JSON response ─────────────────────────────────────────────────
            if "json" in content_type:
                try:
                    data = resp.json()
                    # Handle {"reviews": [...]} or a bare list
                    items = data if isinstance(data, list) else data.get("reviews", data.get("Reviews", []))
                    rows = []
                    for r in items:
                        rows.append({
                            "shop_name":     shop["name"],
                            "shop_type":     shop["type"],
                            "reviewer_name": r.get("Name", r.get("name", r.get("ReviewerName", ""))),
                            "review_date":   r.get("Date", r.get("date", r.get("ReviewDate", ""))),
                            "review_text":   r.get("Text", r.get("text", r.get("Comment", r.get("comment", "")))),
                            "star_rating":   r.get("Rating", r.get("rating", r.get("Stars", ""))),
                            "scraped_date":  TODAY,
                        })
                    if rows:
                        log.info(f"    Reviews loaded (JSON) from {url}")
                        return rows
                except (json.JSONDecodeError, AttributeError):
                    pass

            # ── HTML fragment response ────────────────────────────────────────
            soup2 = BeautifulSoup(resp.text, "html.parser")
            rows = _parse_reviews_from_soup(soup2, shop)
            if rows:
                log.info(f"    Reviews loaded (HTML) from {url}")
                return rows

        except requests.RequestException:
            continue

    return []


def fetch_shop_data(shop: dict) -> tuple[list[dict], list[dict], dict]:
    """
    Fetch a single shop page.
    Returns (items, reviews, enriched_shop_dict).
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

    items = _parse_items_from_page(html, shop)

    # ── Reviews: try inline HTML first, then AJAX endpoints ───────────────────
    reviews = _parse_reviews_from_soup(soup, shop)

    if not reviews:
        shop_id    = _extract_shop_id(html)
        sniffed    = _sniff_reviews_ajax_url(html)
        if sniffed:
            log.info(f"    Sniffed reviews URL: {sniffed}")
        if shop_id or sniffed:
            log.info(f"    shop_id={shop_id} – trying AJAX review endpoints")
            reviews = _fetch_reviews_via_api(shop_id or "", shop, sniffed_url=sniffed)
            if not reviews:
                log.warning(f"    All review endpoints returned 0 reviews for {shop['name']} (shop_id={shop_id})")
        else:
            log.warning(f"    Could not extract shop_id for {shop['name']} – reviews skipped")

    shop["scraped_date"] = TODAY
    return items, reviews, shop


# ──────────────────────────────────────────────────────────────────────────────
# S3 upload
# ──────────────────────────────────────────────────────────────────────────────
def upload_df_to_s3(df: pd.DataFrame, s3: "boto3.client", key: str):
    """Serialize a DataFrame as UTF-8 CSV and put it in S3."""
    buf = StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    try:
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = buf.getvalue().encode("utf-8"),
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
    log.info(f"Run date : {TODAY}")
    log.info(f"S3 bucket: {S3_BUCKET}")

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

    # ── 3. Process each type ──────────────────────────────────────────────────
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
                enriched.append(shop)
                continue

            items, reviews, updated_shop = fetch_shop_data(shop)
            all_items.extend(items)
            all_reviews.extend(reviews)
            enriched.append(updated_shop)

            log.info(f"         items={len(items)}  reviews={len(reviews)}")
            time.sleep(REQUEST_DELAY)

        # S3 key prefix: bleems-data/year=2026/month=02/day=21/Flowers/
        prefix = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type}"

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
            f"  Done {shop_type}: {len(enriched)} shops | "
            f"{len(all_items)} items | {len(all_reviews)} reviews"
        )

    log.info("\nAll done!")


if __name__ == "__main__":
    main()
