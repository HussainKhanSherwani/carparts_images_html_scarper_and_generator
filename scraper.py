"""
scraper.py
Direct port of the original working script's parsing/merging logic.
New additions: hidden SKU extraction, Cloudinary image download, text export.
"""

import re
import html
import time
import urllib.parse
import http.client
import requests
from bs4 import BeautifulSoup

CLOUDINARY_BASE = (
    "https://res.cloudinary.com/us-auto-parts-network-inc"
    "/image/upload/images/{sku}_{img_no}"
)

# ══════════════════════════════════════════════════════════════════════════════
# 1. URL UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def extract_item_number(ebay_url: str):
    match = re.search(r'/itm/(?:[^/]*?-)?(\d{10,13})(?:[/?]|$)', ebay_url)
    if match:
        return match.group(1)
    match = re.search(r'(\d{10,13})', ebay_url)
    return match.group(1) if match else None


def parse_links(raw: str) -> list:
    links = []
    for line in re.split(r'[\n,]+', raw):
        url = line.strip()
        if url.startswith("http") and "ebay.com" in url:
            links.append(url)
    return links


# ══════════════════════════════════════════════════════════════════════════════
# 2. HTTP HELPERS — exact port from original
# ══════════════════════════════════════════════════════════════════════════════

def fetch_html_scrapingant(url: str, api_key: str):
    try:
        if not isinstance(url, str):
            url = str(url)
        encoded_url = urllib.parse.quote(url.strip(), safe='')
        conn = http.client.HTTPSConnection("api.scrapingant.com", timeout=40)
        conn.request("GET", f"/v2/general?url={encoded_url}&x-api-key={api_key}&browser=false")
        res  = conn.getresponse()
        data = res.read()
        if res.status != 200:
            return None
        return data.decode("utf-8", errors="replace")
    except Exception:
        return None


def fetch_url_standard(url: str):
    try:
        if not isinstance(url, str):
            url = str(url)
        headers  = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US"}
        response = requests.get(url.strip(), headers=headers, timeout=15)
        if response.status_code == 200:
            response.encoding = "utf-8"
            return response.text
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 3. HIDDEN SKU EXTRACTION  (new — not in original)
# ══════════════════════════════════════════════════════════════════════════════

def _find_hidden_span(html_str: str):
    """
    Returns text of first hidden span that looks like a SKU.
    Handles: display:none, display: none, visibility:hidden, visibility: hidden
    Also checks for CSS class-based hiding.
    """
    if not html_str:
        return None
    soup = BeautifulSoup(html_str, "html.parser")
    for span in soup.find_all("span"):
        # Normalise style — remove all spaces around colons/semicolons
        raw_style = span.get("style", "")
        norm      = re.sub(r'\s+', '', raw_style).lower()
        hidden    = (
            "display:none"       in norm or
            "visibility:hidden"  in norm or
            "font-size:0"        in norm or
            "color:transparent"  in norm
        )
        # Also catch spans with hidden/sr-only CSS classes
        classes = " ".join(span.get("class", [])).lower()
        if not hidden:
            hidden = any(c in classes for c in ("hidden", "sr-only", "visually-hidden", "hide"))

        if hidden:
            text = span.get_text(strip=True)
            # Must look like a SKU: alphanumeric, no spaces, right length
            if text and 3 < len(text) < 60 and not text.lower().startswith(("http", "www", "ebay")):
                return text
    return None


def get_hidden_sku(main_html: str, api_key: str, item_id: str):
    """
    Extracts hidden SKU BEFORE any span removal.
    Priority: description iframe first (most reliable), then main page.
    """
    # 1. Get iframe URL from main page
    iframe_url = extract_iframe_url(main_html)
    if not iframe_url:
        # Try ScrapingAnt version of main page
        ant_html   = fetch_html_scrapingant(f"https://www.ebay.com/itm/{item_id}", api_key)
        iframe_url = extract_iframe_url(ant_html)

    # 2. Search iframe content first — SKU is usually here
    if iframe_url:
        iframe_html = fetch_url_standard(iframe_url)
        if not iframe_html or len(iframe_html) < 100:
            iframe_html = fetch_html_scrapingant(iframe_url, api_key)
        sku = _find_hidden_span(iframe_html)
        if sku:
            return sku

    # 3. Fall back to main page
    sku = _find_hidden_span(main_html)
    if sku:
        return sku

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 4. CLOUDINARY IMAGE DOWNLOAD  (new — not in original)
# ══════════════════════════════════════════════════════════════════════════════

def download_cloudinary_images(sku: str) -> dict:
    """Downloads images 1–10. Returns {img_no_str: bytes}."""
    results = {}
    for img_no in range(1, 11):
        url = CLOUDINARY_BASE.format(sku=sku, img_no=img_no)
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200 and len(r.content) > 500:
                results[str(img_no)] = r.content
            else:
                if img_no > 1:
                    break
        except Exception:
            break
        time.sleep(0.15)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 5. EBAY IMAGES — exact port from original
# ══════════════════════════════════════════════════════════════════════════════

def parse_images_from_html(html_content: str) -> list:
    if not html_content:
        return []
    soup = BeautifulSoup(html_content, "html.parser")
    grid = soup.find("div", {"class": "ux-image-grid"})
    urls = []
    if grid:
        for btn in grid.find_all("button", {"class": "ux-image-grid-item"}):
            img = btn.find("img")
            if img:
                src = img.get("src") or img.get("data-src")
                if src and "DOcAAOSw8NplLtwK" not in src:
                    src = src.replace("s-l140", "s-l1600")
                    src = re.sub(r's-l\d+', 's-l1600', src)
                    urls.append(src)
    return urls


def get_ebay_images(item_id: str, api_key: str) -> list:
    url          = f"https://www.ebay.com/itm/{item_id}"
    html_content = fetch_url_standard(url)
    images       = parse_images_from_html(html_content)
    if not images:
        html_content = fetch_html_scrapingant(url, api_key)
        images       = parse_images_from_html(html_content)
    return images[:6]


# ══════════════════════════════════════════════════════════════════════════════
# 6. IFRAME / DESCRIPTION — exact port from original (fetch_iframe_html)
# ══════════════════════════════════════════════════════════════════════════════

def extract_iframe_url(html_content: str):
    if not html_content:
        return None
    soup   = BeautifulSoup(html_content, "html.parser")
    iframe = soup.find("iframe", id="desc_ifr")
    if iframe and iframe.get("src"):
        return iframe.get("src")
    return None


def fetch_description_html(product_url: str, api_key: str):
    """Exact port of fetch_iframe_html from original."""
    main_html  = fetch_url_standard(product_url)
    iframe_url = extract_iframe_url(main_html)
    if not iframe_url:
        main_html  = fetch_html_scrapingant(product_url, api_key)
        iframe_url = extract_iframe_url(main_html)
    if not iframe_url:
        return None
    iframe_content = fetch_url_standard(iframe_url)
    if not iframe_content or len(iframe_content) < 100:
        iframe_content = fetch_html_scrapingant(iframe_url, api_key)
    return iframe_content




# ══════════════════════════════════════════════════════════════════════════════
# 6b. EBAY MOTORS COMPATIBILITY TABLE (main page + pagination)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_compat_table_page(html_str: str) -> list:
    """
    Parses one page of the motors compatibility table.
    Returns list of dicts: {year, make, model, trim, engine, notes}
    """
    if not html_str:
        return []
    soup = BeautifulSoup(html_str, "html.parser")
    rows = []
    table = soup.find("table", {"class": lambda c: c and "ux-table-section" in c})
    if not table:
        return []
    for tr in table.find("tbody").find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue
        def _cell(i):
            # Get only the first ux-textspans inside the cell (ignore Read more/less buttons)
            spans = cells[i].find_all("span", {"class": "ux-textspans"})
            # First span is the real text, rest are button labels
            return spans[0].get_text(strip=True) if spans else ""
        rows.append({
            "year":   _cell(0),
            "make":   _cell(1),
            "model":  _cell(2),
            "trim":   _cell(3),
            "engine": _cell(4),
            "notes":  _cell(5) if len(cells) > 5 else "",
        })
    return rows


def _get_compat_page_count(html_str: str) -> int:
    """Returns total number of pagination pages from the compat table."""
    if not html_str:
        return 1
    soup  = BeautifulSoup(html_str, "html.parser")
    items = soup.select(".motors-pagination .pagination__items .pagination__item")
    nums  = []
    for item in items:
        try:
            nums.append(int(item.get_text(strip=True)))
        except ValueError:
            pass
    return max(nums) if nums else 1


def _fetch_compat_json(item_id: str, offset: int, session_cookies: dict,
                        seller_name: str = "", category_id: str = "") -> list:
    """
    Fetches compatibility rows via eBay /g/api/finders POST with session cookies.
    Returns list of row dicts.
    """
    import json as _json
    url = (
        "https://www.ebay.com/g/api/finders"
        f"?module_groups=PART_FINDER&referrer=VIEWITEM"
        f"&offset={offset}&module=COMPATIBILITY_TABLE"
    )
    headers = {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Origin":        "https://www.ebay.com",
        "Referer":       f"https://www.ebay.com/itm/{item_id}",
        "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "x-ebay-c-marketplace-id":  "EBAY-US",
        "x-ebay-c-tracking-config": "viewTrackingEnabled=true,perfTrackingEnabled=true",
    }
    payload = {
        "scopedContext": {
            "catalogDetails": {
                "itemId":        item_id,
                "sellerName":    seller_name,
                "categoryId":    category_id or "262146",
                "marketplaceId": "EBAY-US",
            }
        }
    }
    try:
        resp = _requests.post(
            url, json=payload, headers=headers,
            cookies=session_cookies, timeout=15
        )
        if resp.status_code != 200:
            return []
        data     = resp.json()
        rows_raw = (
            data.get("modules", {})
                .get("COMPATIBILITY_TABLE", {})
                .get("paginatedTable", {})
                .get("rows", [])
        )
        rows = []
        for row in rows_raw:
            cells = row.get("cells", [])
            def _cell_text(c):
                spans = (
                    c.get("textSpans") or
                    (c.get("textualDisplays") or [{}])[0].get("textSpans", [])
                )
                return spans[0].get("text", "") if spans else ""
            if len(cells) >= 5:
                rows.append({
                    "year":   _cell_text(cells[0]),
                    "make":   _cell_text(cells[1]),
                    "model":  _cell_text(cells[2]),
                    "trim":   _cell_text(cells[3]),
                    "engine": _cell_text(cells[4]),
                    "notes":  _cell_text(cells[5]) if len(cells) > 5 else "",
                })
        return rows
    except Exception:
        return []


def scrape_compatibility_table(item_id: str, main_html: str, api_key: str,
                               session_cookies: dict = None) -> list:
    """
    Scrapes ALL pages of the eBay motors compatibility table.
    Page 1: parsed from main_html (reliable).
    Page 2+: POST to /g/api/finders with session cookies obtained from a fresh GET.
    """
    import re as _re

    # Page 1 from main HTML
    all_rows    = _parse_compat_table_page(main_html)
    total_pages = _get_compat_page_count(main_html)

    if total_pages <= 1:
        return all_rows

    # Extract meta from main page
    seller_name = ""
    category_id = ""
    try:
        m = _re.search(r'"sellerName"\s*:\s*"([^"]+)"', main_html)
        if m:
            seller_name = m.group(1)
        m2 = _re.search(r'"categoryId"\s*:\s*"([^"]+)"', main_html)
        if m2:
            category_id = m2.group(1)
    except Exception:
        pass

    # Get fresh session cookies by doing a real GET on the item page
    cookies = {}
    try:
        r = _requests.get(
            f"https://www.ebay.com/itm/{item_id}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
            timeout=15,
            allow_redirects=True,
        )
        cookies = dict(r.cookies)
    except Exception:
        pass

    PAGE_SIZE = 20
    for page in range(2, total_pages + 1):
        offset = (page - 1) * PAGE_SIZE
        rows   = _fetch_compat_json(item_id, offset, cookies, seller_name, category_id)
        if rows:
            all_rows.extend(rows)
        time.sleep(0.5)

    return all_rows




def extract_item_specs(html_str: str) -> dict:
    """
    Extracts the full item specs table from eBay listing using text-search approach.
    Returns dict of all label->value pairs found, plus a 'part_link_number' key
    that tries multiple known field name variants.
    """
    if not html_str:
        return {}
    import re as _re
    soup = BeautifulSoup(html_str, "html.parser")

    specs = {}

    # Generic: find all dl.ux-labels-values and extract label->value pairs
    for dl in soup.find_all("dl", {"data-testid": "ux-labels-values"}):
        dt = dl.find("dt")
        dd = dl.find("dd")
        if dt and dd:
            label = dt.get_text(strip=True)
            # For dd, get first ux-textspans (skip Read more/less button text)
            spans = dd.find_all("span", {"class": "ux-textspans"})
            value = spans[0].get_text(strip=True) if spans else dd.get_text(strip=True)
            if label and value:
                specs[label] = value

    # Resolve Part Link Number from known variants (most specific first)
    part_link_variants = [
        "Part Link Number", "Parts Link Number",
        "Partslink Number", "Replaces Partslink Number",
    ]
    part_link_number = None
    for variant in part_link_variants:
        # Try exact key first
        if variant in specs:
            part_link_number = specs[variant]
            break
        # Try case-insensitive text search as fallback
        tag = soup.find(string=_re.compile(rf"^{re.escape(variant)}$", _re.I))
        if tag:
            nxt = tag.find_next("span", {"class": "ux-textspans"})
            if nxt:
                part_link_number = nxt.get_text(strip=True)
                break

    specs["_part_link_number"] = part_link_number
    return specs


def extract_part_link_number(html_str: str) -> str | None:
    """Convenience wrapper — returns just the part link number."""
    return extract_item_specs(html_str).get("_part_link_number")


# ══════════════════════════════════════════════════════════════════════════════
# 7. MASTER SCRAPE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_ebay_item(ebay_url: str, api_key: str) -> dict:
    result = {
        "item_id":          None,
        "hidden_sku":       None,
        "part_link_number": None,
        "item_specs":       {},
        "seller_name":      None,
        "gallery_imgs":     [],
        "cloud_images":     {},
        "desc_html":        None,
        "span_texts":       [],
        "compat_rows":      [],
    }

    item_id = extract_item_number(ebay_url)
    if not item_id:
        return result
    result["item_id"] = item_id

    # Fetch main page once via standard requests
    item_url  = f"https://www.ebay.com/itm/{item_id}"
    main_html = fetch_url_standard(item_url)

    # If standard fetch didn't get specs (JS-rendered), use ScrapingAnt ONCE
    # and reuse that html for everything (gallery, specs, compat)
    specs = extract_item_specs(main_html) if main_html else {}
    if not specs.get("_part_link_number"):
        main_html = fetch_html_scrapingant(item_url, api_key) or main_html

    # Seller name (from main HTML JSON-LD)
    import re as _re2
    m = _re2.search(r'"sellerName"\s*:\s*"([^"]+)"', main_html or "")
    if m:
        result["seller_name"] = m.group(1)

    # Extract specs from whichever main_html we ended up with
    specs = extract_item_specs(main_html)
    result["part_link_number"] = specs.get("_part_link_number")
    result["item_specs"]       = {k: v for k, v in specs.items() if not k.startswith("_")}

    # Motors compatibility table (all pages)
    result["compat_rows"] = scrape_compatibility_table(item_id, main_html, api_key)

    # eBay gallery images
    result["gallery_imgs"] = parse_images_from_html(main_html)
    if not result["gallery_imgs"]:
        result["gallery_imgs"] = get_ebay_images(item_id, api_key)

    # Description iframe — SKU is extracted here during parsing
    desc_html = fetch_description_html(ebay_url, api_key)
    result["desc_html"] = desc_html

    # Extract SKU by parsing description (spans captured before removal)
    if desc_html:
        desc_soup         = BeautifulSoup(desc_html, "html.parser")
        _nodes, found_sku, all_spans = clean_description_carparts(desc_soup)
        result["hidden_sku"]  = found_sku
        result["span_texts"]  = all_spans
        if found_sku:
            result["cloud_images"] = download_cloudinary_images(found_sku)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# 8. PARSING — exact port of clean_description_carparts from original
# ══════════════════════════════════════════════════════════════════════════════

def clean_description_carparts(soup: BeautifulSoup) -> tuple:
    """
    Returns (nodes: list, found_sku: str|None).
    SKU is extracted from hidden spans DURING cleaning, before they are removed.
    """
    out_soup  = BeautifulSoup("", "html.parser")
    raw_nodes = []
    found_sku = [None]   # mutable so inner function can write to it

    container  = soup.find(id="content__right") or soup.find("section", id="content__right") or soup
    start_node = None
    for h in container.find_all(["h2", "h1", "h3", "h4"]):
        if "Description" in h.get_text(strip=True):
            start_node = h
            break
    if not start_node:
        return [], None, []

    # ── PRE-PASS: scan ALL spans in the container for SKU BEFORE any removal ──
    # This catches SKUs in divs, spans not inside p/h3, anywhere in description
    all_span_texts = []
    for span in container.find_all("span"):
        txt = span.get_text(strip=True)
        if txt:
            all_span_texts.append(txt)
        if (txt and 3 < len(txt) < 60
                and not txt.lower().startswith(('http', 'www', 'ebay', 'see ', 'click'))
                and found_sku[0] is None):
            found_sku[0] = txt
    # Store all span texts for logging (accessible via return value)
    found_sku.append(all_span_texts)   # found_sku[1] = all span texts

    def fix_mojibake(text):
        if not text:
            return ""
        try:
            text = text.encode('cp1252').decode('utf-8')
        except Exception:
            try:
                text = text.encode('latin1').decode('utf-8')
            except Exception:
                pass
        replacements = {
            "â": "'",
            "â": '"',
            "â": '"',
            "â": "-",
            "â": "-",
            "Â": " ",
            "â¦": "...",
        }
        for bad, good in replacements.items():
            text = text.replace(bad, good)
        return text.strip()

    def process_node(node):
        if node.name is None:
            return []
        if node.name == 'section':
            return ["STOP"]
        if 'desc__list' in node.get('class', []) or "Terms of Use" in node.get_text():
            return []

        raw_text_lower = node.get_text(" ", strip=True).lower()
        is_capa        = "capa certified" in raw_text_lower

        if not is_capa:
            if "use existing emblem" in raw_text_lower:
                return []

        if node.name == 'div' or node.find(['ul', 'div', 'p']):
            unpacked = []
            for child in node.children:
                res = process_node(child)
                if "STOP" in res:
                    return ["STOP"]
                unpacked.extend(res)
            return unpacked

        if node.name == 'ul':
            new_ul = out_soup.new_tag("ul")
            for li in node.find_all('li'):
                if li.text.strip():
                    new_li        = out_soup.new_tag("li")
                    new_li.string = fix_mojibake(li.text)
                    new_ul.append(new_li)
            return [new_ul] if new_ul.contents else []

        if node.name in ['p', 'h3', 'h4', 'h5', 'h6']:
            # Capture spans BEFORE removing — SKU lives here as hidden span
            for span in node.find_all("span"):
                span_text = span.get_text(strip=True)
                if (span_text and 3 < len(span_text) < 60
                        and not span_text.lower().startswith(('http', 'www', 'ebay'))
                        and found_sku[0] is None):
                    found_sku[0] = span_text
                span.decompose()
            txt = fix_mojibake(node.get_text(" ", strip=True))
            if not txt:
                return []
            if node.name in ['h3', 'h4', 'h5', 'h6']:
                tag        = out_soup.new_tag("h3")
                tag.string = txt
            else:
                tag = out_soup.new_tag("p")
                if node.find("strong") or node.find("b"):
                    strong        = out_soup.new_tag("strong")
                    strong.string = txt
                    tag.append(strong)
                else:
                    tag.string = txt
            return [tag]

        return []

    # Step A: collect raw nodes
    for tag in start_node.next_siblings:
        results = process_node(tag)
        if "STOP" in results:
            break
        raw_nodes.extend(results)

    # Step B: deduplicate headers
    strong_texts  = set()
    deduped_nodes = []
    for node in raw_nodes:
        if node.name == 'p' and node.find('strong'):
            strong_texts.add(node.get_text(strip=True).lower())
    for node in raw_nodes:
        if node.name == 'h3':
            h3_text = node.get_text(strip=True).lower()
            if h3_text in strong_texts and "capa certified" not in h3_text:
                continue
        deduped_nodes.append(node)

    # Step C: final formatting
    final_nodes = []
    count       = len(deduped_nodes)
    for i, node in enumerate(deduped_nodes):
        if node.name == 'p':
            strong_tag = node.find('strong')
            if strong_tag and len(strong_tag.get_text(strip=True)) >= len(node.get_text(strip=True)) - 2:
                new_h3        = out_soup.new_tag("h3")
                new_h3.string = node.get_text(strip=True)
                final_nodes.append(new_h3)
                continue
            if len(node.get_text(strip=True)) < 50 and i + 1 < count:
                if deduped_nodes[i + 1].name == 'ul':
                    node['style'] = "font-weight: bold;"
            final_nodes.append(node)
        else:
            final_nodes.append(node)

    all_spans = found_sku[1] if len(found_sku) > 1 else []
    return final_nodes, found_sku[0], all_spans


# ══════════════════════════════════════════════════════════════════════════════
# 9. COMPATIBILITY — exact port from original
# ══════════════════════════════════════════════════════════════════════════════

def extract_compatibility_carparts(soup: BeautifulSoup, template: BeautifulSoup):
    inner_div          = template.new_tag("div")
    inner_div['class'] = "compat-grid"

    container = soup.find("div", class_="item__list")
    if not container:
        return None

    for block in container.find_all("div", class_="items__list--content"):
        for child in block.find_all(["p", "ul"], recursive=False):
            if child.name == "p":
                new_p         = template.new_tag("p")
                strong        = template.new_tag("strong")
                strong.string = child.get_text(strip=True)
                new_p.append(strong)
                inner_div.append(new_p)
            elif child.name == "ul":
                new_ul = template.new_tag("ul")
                for li in child.find_all("li"):
                    new_li        = template.new_tag("li")
                    new_li.string = li.get_text(" ", strip=True)
                    new_ul.append(new_li)
                inner_div.append(new_ul)

    return inner_div


# ══════════════════════════════════════════════════════════════════════════════
# 10. CSS — exact port from original inject_compact_table_css
# ══════════════════════════════════════════════════════════════════════════════

def inject_compact_table_css(template_soup: BeautifulSoup):
    style_tag = template_soup.find("style")
    if not style_tag:
        style_tag = template_soup.new_tag("style")
        if template_soup.head:
            template_soup.head.append(style_tag)
        else:
            template_soup.body.insert(0, style_tag)

    css_code = """
        .table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .table td {
            width: 25%;
            padding: 8px;
            border: 1px solid #eee;
            font-size: 14px;
        }
        .table tr td:nth-child(1), .table tr td:nth-child(3) {
            font-weight: bold; color: #333;
        }
        .table tr td:nth-child(2), .table tr td:nth-child(4) {
            color: #555;
        }
        .table tr:nth-child(odd) td  { background-color: #fff; }
        .table tr:nth-child(even) td { background-color: #f2f2f2; }
    """
    if style_tag.string:
        style_tag.string += css_code
    else:
        style_tag.string = css_code


# ══════════════════════════════════════════════════════════════════════════════
# 11. HTML MERGE — exact port of merge_all_data from original
# ══════════════════════════════════════════════════════════════════════════════

def merge_all_data(template_str: str, source_data_html: str, image_urls: list) -> str:
    template = BeautifulSoup(template_str,     "html.parser")
    data     = BeautifulSoup(source_data_html, "html.parser")

    inject_compact_table_css(template)

    def strip_styles(tag):
        if hasattr(tag, 'attrs'):
            tag.attrs = {}
        for child in tag.find_all(True):
            child.attrs = {}

    # 1. Images
    if image_urls:
        img_box = template.find("div", class_="product-image-box")
        if img_box:
            img_box.clear()
            for i, url in enumerate(image_urls):
                idx = i + 1
                inp = template.new_tag("input", attrs={"type": "radio", "name": "gal", "id": f"gal{idx}"})
                if i == 0:
                    inp.attrs["checked"] = ""
                img_box.append(inp)
                div = template.new_tag("div", attrs={"id": f"content{idx}", "class": "product-image-container"})
                div.append(template.new_tag("img", attrs={"src": url}))
                img_box.append(div)
            thumb_box = template.new_tag("div", attrs={"class": "thumbnails-box"})
            for i, url in enumerate(image_urls):
                idx = i + 1
                lbl = template.new_tag("label", attrs={"for": f"gal{idx}", "class": "thumb-label"})
                lbl.append(template.new_tag("img", attrs={"src": url.replace("s-l1600", "s-l140")}))
                thumb_box.append(lbl)
            img_box.append(thumb_box)

    # 2. Title
    title = data.select_one(".eb_title")
    if title and template.select_one(".title h1"):
        template.select_one(".title h1").string = title.get_text(strip=True)

    # 3. Description
    desc_box                = template.select_one('.middle-right .description-details')
    cleaned_desc, _desc_sku, _ = clean_description_carparts(data)
    if desc_box:
        desc_box.clear()
        links = BeautifulSoup(
            '<div style="font-weight:bold;padding-bottom:10px">'
            '<a href="https://www.ebay.com/str/hiveofdeals?_tab=about" target="_blank" style="color:#0053a0">Terms of Use</a>'
            ' | '
            '<a href="https://www.ebay.com/str/hiveofdeals?_tab=about" target="_blank" style="color:#0053a0">Warranty Coverage Policy</a>'
            '</div>',
            "html.parser",
        )
        desc_box.append(links)
        for c in cleaned_desc:
            desc_box.append(c)

    # 4. TABLE (Double-Up Logic) — exact port
    t_body  = template.select_one("table.table tbody")
    s_table = data.find(id="content__bottom")

    if t_body and s_table and s_table.find("table"):
        t_body.clear()
        all_pairs = []
        for row in s_table.find("table").find_all("tr"):
            cells = row.find_all(['td', 'th'])
            if len(cells) == 2:
                strip_styles(cells[0])
                strip_styles(cells[1])
                all_pairs.append((cells[0], cells[1]))

        for i in range(0, len(all_pairs), 2):
            new_row = template.new_tag("tr")
            new_row.append(all_pairs[i][0])
            new_row.append(all_pairs[i][1])
            if i + 1 < len(all_pairs):
                new_row.append(all_pairs[i + 1][0])
                new_row.append(all_pairs[i + 1][1])
            else:
                new_row.append(template.new_tag("td"))
                new_row.append(template.new_tag("td"))
            t_body.append(new_row)

    # 5. Compatibility
    all_d  = template.find_all("div", class_="description")
    compat = next((d for d in all_d if d.find("h4") and "Compatible" in d.find("h4").text), None)
    if compat:
        det   = compat.find("div", class_="description-details-1")
        c_div = extract_compatibility_carparts(data, template)
        if det and c_div:
            det.clear()
            det.append(c_div)

    # 6. Notes — exact port
    notes_target_div = None
    red_warning      = template.find("p", style=lambda s: s and "var(--red)" in s)
    if red_warning:
        notes_target_div = red_warning.parent

    if notes_target_div:
        source_notes_header = data.find("h2", string=lambda t: t and "Notes" in t)
        if source_notes_header:
            curr = source_notes_header.next_sibling
            while curr:
                if curr.name == 'div' and 'content__table-wrap' in curr.get('class', []):
                    break
                if curr.name in ['h2', 'h1', 'section']:
                    break
                if curr.name == 'p':
                    note_text = curr.get_text(strip=True)
                    t_lower   = note_text.lower()
                    if "brand new in the box" in t_lower and "quality guaranteed" in t_lower:
                        curr = curr.next_sibling
                        continue
                    if note_text:
                        new_p        = template.new_tag("p")
                        new_p.string = note_text
                        notes_target_div.append(new_p)
                curr = curr.next_sibling

    return html.unescape(str(template))


# ══════════════════════════════════════════════════════════════════════════════
# 12. TEXT EXPORT  (new — not in original)
# ══════════════════════════════════════════════════════════════════════════════

def extract_text_data(source_data_html: str, compat_rows: list = None) -> str:
    """Exports all listing sections to a clean plain-text file."""
    soup  = BeautifulSoup(source_data_html, "html.parser")
    lines = []
    sep   = "=" * 60

    # Title
    lines += [sep, "TITLE", sep]
    title = soup.select_one(".eb_title")
    lines.append(title.get_text(strip=True) if title else "(not found)")
    lines.append("")

    # Description
    lines += [sep, "DESCRIPTION", sep]
    _desc_nodes, _, _ = clean_description_carparts(soup)
    for node in _desc_nodes:
        if node.name in ["h2", "h3"]:
            lines.append(f"\n[[ {node.get_text(strip=True)} ]]")
        elif node.name == "ul":
            for li in node.find_all("li"):
                lines.append(f"  - {li.get_text(strip=True)}")
        elif node.name == "p":
            lines.append(node.get_text(strip=True))
    lines.append("")

    # Notes
    lines += [sep, "NOTES", sep]
    notes_h = soup.find("h2", string=lambda t: t and "Notes" in t)
    found   = False
    if notes_h:
        curr = notes_h.next_sibling
        while curr:
            if curr.name == 'div' and 'content__table-wrap' in curr.get('class', []):
                break
            if curr.name in ['h2', 'h1', 'section']:
                break
            if curr.name == 'p':
                note_text = curr.get_text(strip=True)
                t_lower   = note_text.lower()
                if "brand new in the box" in t_lower and "quality guaranteed" in t_lower:
                    curr = curr.next_sibling
                    continue
                if note_text:
                    lines.append(note_text)
                    found = True
            curr = curr.next_sibling
    if not found:
        lines.append("(none)")
    lines.append("")

    # Specifications
    lines += [sep, "SPECIFICATIONS", sep]
    s_table = soup.find(id="content__bottom")
    if s_table and s_table.find("table"):
        for row in s_table.find("table").find_all("tr"):
            cells = row.find_all(['td', 'th'])
            if len(cells) == 2:
                lines.append(f"{cells[0].get_text(strip=True):<30} {cells[1].get_text(strip=True)}")
    else:
        lines.append("(not found)")
    lines.append("")

    # Compatibility — iframe-based (carparts format)
    lines += [sep, "COMPATIBILITY (Description)", sep]
    container = soup.find("div", class_="item__list")
    if container:
        for block in container.find_all("div", class_="items__list--content"):
            for child in block.find_all(["p", "ul"], recursive=False):
                if child.name == "p":
                    lines.append(f"\n{child.get_text(strip=True)}:")
                elif child.name == "ul":
                    for li in child.find_all("li"):
                        lines.append(f"  - {li.get_text(' ', strip=True)}")
    else:
        lines.append("(not found)")
    lines.append("")

    # eBay Motors Compatibility Table (all pages from main listing)
    lines += [sep, "FITMENT TABLE", sep]
    if compat_rows:
        # Header
        lines.append(f"{'Year':<6} {'Make':<16} {'Model':<10} {'Trim':<30} {'Engine':<45} Notes")
        lines.append("-" * 120)
        for r in compat_rows:
            line = (
                f"{r.get('year',''):<6} "
                f"{r.get('make',''):<16} "
                f"{r.get('model',''):<10} "
                f"{r.get('trim',''):<30} "
                f"{r.get('engine',''):<45} "
                f"{r.get('notes','')}"
            )
            lines.append(line)
        lines.append(f"\nTotal: {len(compat_rows)} vehicle(s)")
    else:
        lines.append("(not found)")
    lines.append("")

    return "\n".join(lines)