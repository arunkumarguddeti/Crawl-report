"""
crawler.py — Async Website Deep Crawler with Broken Link Detection
Generates a CSV data file + self-contained HTML dashboard report.

Configuration: edit the CONFIG block below, or pass env vars / CLI args.
Usage:
    python crawler.py
    BASE_URL=https://example.com python crawler.py
"""

import asyncio
import aiohttp
import csv
import json
import os
import sys
import time
import logging
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qs
from urllib.robotparser import RobotFileParser
from pathlib import Path

# ─────────────────────────────────────────────────────────────
#  CONFIG  — edit these or override with environment variables
# ─────────────────────────────────────────────────────────────
CONFIG = {
    "BASE_URL":        os.getenv("BASE_URL",    "https://www.example.com"),
    "MAX_DEPTH":       int(os.getenv("MAX_DEPTH",   "10")),
    "MAX_PAGES":       int(os.getenv("MAX_PAGES",   "5000")),   # hard cap
    "CONCURRENCY":     int(os.getenv("CONCURRENCY", "10")),     # async workers
    "TIMEOUT":         int(os.getenv("TIMEOUT",     "10")),     # seconds per request
    "POLITE_DELAY":  float(os.getenv("POLITE_DELAY","0.3")),    # seconds between batches
    "RESPECT_ROBOTS":  os.getenv("RESPECT_ROBOTS", "true").lower() == "true",
    "OUTPUT_DIR":      os.getenv("OUTPUT_DIR",  "./reports"),
    "USER_AGENT":      "Mozilla/5.0 (compatible; SiteCrawler/1.0; +https://github.com/your-repo)",
    # Query params to strip when normalising URLs (avoid infinite pagination traps)
    "STRIP_PARAMS":    {"utm_source","utm_medium","utm_campaign","utm_content",
                        "utm_term","sessionid","PHPSESSID","sid","ref"},
    # File extensions to skip parsing (still HEAD-check, just don't crawl for more links)
    "SKIP_PARSE_EXTS": {".pdf",".zip",".docx",".xlsx",".pptx",".exe",".dmg",
                        ".mp4",".mp3",".avi",".mov",".jpg",".jpeg",".png",
                        ".gif",".svg",".webp",".ico",".woff",".woff2",".ttf"},
    # Schemes that are not HTTP — skip entirely
    "SKIP_SCHEMES":    {"mailto","tel","javascript","data","ftp","sms","callto"},
}

# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("crawler")


# ──────────────────────────────────────────────
#  URL HELPERS
# ──────────────────────────────────────────────

def normalize(url: str, base: str = "") -> str | None:
    """
    Clean a URL:
    - Resolve relative URLs against `base`
    - Strip fragments (#section)
    - Lowercase scheme + host
    - Strip tracking/session query params
    - Return None for non-HTTP schemes or empty strings
    """
    if not url:
        return None
    url = url.strip()

    # Resolve relative URLs
    if base:
        url = urljoin(base, url)

    try:
        p = urlparse(url)
    except Exception:
        return None

    # Skip non-HTTP(S) schemes
    scheme = p.scheme.lower()
    if scheme in CONFIG["SKIP_SCHEMES"]:
        return None
    if scheme not in ("http", "https"):
        return None

    # Strip fragment, lowercase scheme+host, clean params
    params = parse_qs(p.query, keep_blank_values=True)
    cleaned = {k: v for k, v in params.items()
               if k not in CONFIG["STRIP_PARAMS"]}
    clean_query = urlencode(cleaned, doseq=True)

    clean = urlunparse((
        scheme,
        p.netloc.lower(),
        p.path.rstrip("/") or "/",
        p.params,
        clean_query,
        ""  # no fragment
    ))
    return clean


def is_same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc.lower() == urlparse(base).netloc.lower()


def should_skip_parse(url: str) -> bool:
    """True if the URL points to a binary file we should not parse for links."""
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in CONFIG["SKIP_PARSE_EXTS"])


# ──────────────────────────────────────────────
#  ROBOTS.TXT
# ──────────────────────────────────────────────

def load_robots(base_url: str) -> RobotFileParser | None:
    if not CONFIG["RESPECT_ROBOTS"]:
        return None
    rp = RobotFileParser()
    rp.set_url(base_url.rstrip("/") + "/robots.txt")
    try:
        rp.read()
        log.info("robots.txt loaded from %s", base_url)
    except Exception as e:
        log.warning("Could not read robots.txt: %s", e)
        return None
    return rp


def robots_allow(rp: RobotFileParser | None, url: str) -> bool:
    if rp is None:
        return True
    return rp.can_fetch(CONFIG["USER_AGENT"], url)


# ──────────────────────────────────────────────
#  EFFORT CLASSIFICATION
# ──────────────────────────────────────────────

def effort_level(status) -> str:
    s = str(status)
    if s == "200":                           return "None"
    if s.startswith("3"):                    return "Low"
    if s in ("401", "403", "429"):           return "Low-Medium"
    if s in ("404", "410", "451"):           return "High"
    if s.startswith("5"):                    return "Medium"
    if "Timeout" in s or "SSL" in s:         return "Medium"
    if "Error" in s or "Connection" in s:    return "High"
    return "Medium"

def status_category(status) -> str:
    s = str(status)
    if s == "200":                return "OK"
    if s.startswith("2"):         return "2xx Other"
    if s.startswith("3"):         return "Redirect"
    if s in ("404", "410"):       return "Not Found"
    if s.startswith("4"):         return "4xx Client Error"
    if s.startswith("5"):         return "5xx Server Error"
    if "Timeout" in s:            return "Timeout"
    return "Error"


# ──────────────────────────────────────────────
#  ASYNC HTTP HELPERS
# ──────────────────────────────────────────────

async def fetch_page_html(session: aiohttp.ClientSession, url: str) -> tuple[int, str, str]:
    """
    GET a page. Returns (status_code, html_text, final_url_after_redirects).
    """
    headers = {"User-Agent": CONFIG["USER_AGENT"]}
    timeout = aiohttp.ClientTimeout(total=CONFIG["TIMEOUT"])
    try:
        async with session.get(url, headers=headers, timeout=timeout,
                               allow_redirects=True, ssl=False) as resp:
            try:
                html = await resp.text(errors="ignore")
            except Exception:
                html = ""
            return resp.status, html, str(resp.url)
    except asyncio.TimeoutError:
        return "Timeout", "", url
    except aiohttp.ClientSSLError as e:
        return f"SSL Error", "", url
    except aiohttp.ClientConnectorError as e:
        return f"Connection Error", "", url
    except Exception as e:
        return f"Error: {type(e).__name__}", "", url


async def check_link_status(session: aiohttp.ClientSession,
                             url: str,
                             link_cache: dict) -> tuple:
    """
    HEAD (then GET fallback) a URL to get status + final URL.
    Uses link_cache to avoid duplicate requests.
    """
    if url in link_cache:
        return link_cache[url]

    headers = {"User-Agent": CONFIG["USER_AGENT"]}
    timeout = aiohttp.ClientTimeout(total=CONFIG["TIMEOUT"])
    try:
        async with session.head(url, headers=headers, timeout=timeout,
                                allow_redirects=True, ssl=False) as resp:
            if resp.status == 405:
                # HEAD not allowed — fall back to GET
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=405)
            result = (resp.status, str(resp.url))
    except aiohttp.ClientResponseError:
        # Fallback to GET for 405 or other HEAD failures
        try:
            async with session.get(url, headers=headers, timeout=timeout,
                                   allow_redirects=True, ssl=False) as resp:
                result = (resp.status, str(resp.url))
        except asyncio.TimeoutError:
            result = ("Timeout", url)
        except Exception as e:
            result = (f"Error: {type(e).__name__}", url)
    except asyncio.TimeoutError:
        result = ("Timeout", url)
    except aiohttp.ClientSSLError:
        result = ("SSL Error", url)
    except aiohttp.ClientConnectorError:
        result = ("Connection Error", url)
    except Exception as e:
        result = (f"Error: {type(e).__name__}", url)

    link_cache[url] = result
    return result


# ──────────────────────────────────────────────
#  HTML PARSER  (no BeautifulSoup needed — stdlib)
# ──────────────────────────────────────────────

def extract_links(html: str, page_url: str) -> list[tuple[str, str]]:
    """
    Extract all <a href> links + anchor text from HTML.
    Returns list of (absolute_url, anchor_text) tuples.
    Falls back to BeautifulSoup if available, else uses stdlib html.parser.
    """
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            text = a.get_text(separator=" ", strip=True)[:200]
            norm = normalize(href, page_url)
            if norm:
                links.append((norm, text))
        return links
    except ImportError:
        pass

    # Stdlib fallback using html.parser
    from html.parser import HTMLParser

    class LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []
            self._in_a = False
            self._current_href = None
            self._current_text = []

        def handle_starttag(self, tag, attrs):
            if tag == "a":
                self._in_a = True
                d = dict(attrs)
                href = d.get("href", "").strip()
                self._current_href = normalize(href, page_url)
                self._current_text = []

        def handle_data(self, data):
            if self._in_a:
                self._current_text.append(data.strip())

        def handle_endtag(self, tag):
            if tag == "a" and self._in_a:
                if self._current_href:
                    text = " ".join(t for t in self._current_text if t)[:200]
                    self.links.append((self._current_href, text))
                self._in_a = False
                self._current_href = None
                self._current_text = []

    parser = LinkParser()
    try:
        parser.feed(html)
    except Exception:
        pass
    return parser.links


# ──────────────────────────────────────────────
#  MAIN CRAWL  (BFS + async 10-worker semaphore)
# ──────────────────────────────────────────────

async def crawl() -> list[dict]:
    base_url = CONFIG["BASE_URL"].rstrip("/")
    sem = asyncio.Semaphore(CONFIG["CONCURRENCY"])

    visited_pages: set[str] = set()   # pages we have crawled (HTML fetched)
    link_cache:    dict      = {}     # url -> (status, final_url)
    results:       list[dict]= []

    rp = load_robots(base_url)

    # normalise the entry point
    start = normalize(base_url)
    if not start:
        log.error("BASE_URL '%s' is invalid.", base_url)
        return []

    queue: deque[tuple[str, int]] = deque([(start, 0)])

    connector = aiohttp.TCPConnector(
        limit=CONFIG["CONCURRENCY"] + 5,
        ssl=False,
        force_close=False,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(connector=connector) as session:

        while queue:
            # Drain batch of up to CONCURRENCY items from queue
            batch = []
            while queue and len(batch) < CONFIG["CONCURRENCY"]:
                url, depth = queue.popleft()
                if url in visited_pages:
                    continue
                if len(visited_pages) >= CONFIG["MAX_PAGES"]:
                    log.warning("MAX_PAGES cap (%d) reached — stopping crawl.", CONFIG["MAX_PAGES"])
                    queue.clear()
                    break
                if depth > CONFIG["MAX_DEPTH"]:
                    continue
                if not robots_allow(rp, url):
                    log.info("robots.txt disallows: %s", url)
                    continue
                visited_pages.add(url)
                batch.append((url, depth))

            if not batch:
                break

            log.info("Crawling batch of %d pages  |  total visited: %d  |  queue: %d",
                     len(batch), len(visited_pages), len(queue))

            async def process_page(page_url: str, depth: int):
                async with sem:
                    status, html, final_url = await fetch_page_html(session, page_url)

                if str(status) != "200":
                    results.append({
                        "page_url":   page_url,
                        "link_url":   page_url,
                        "link_text":  "(page itself)",
                        "link_type":  "Page",
                        "status":     status,
                        "final_url":  final_url,
                        "depth":      depth,
                        "effort":     effort_level(status),
                        "category":   status_category(status),
                        "timestamp":  datetime.utcnow().isoformat(timespec="seconds"),
                    })
                    return

                if should_skip_parse(page_url):
                    return

                page_links = extract_links(html, page_url)
                if not page_links:
                    return

                # Check all links on this page concurrently (cached)
                async def check_one(link_url: str, link_text: str):
                    async with sem:
                        lnk_status, lnk_final = await check_link_status(
                            session, link_url, link_cache)
                    link_type = "Internal" if is_same_domain(link_url, base_url) else "External"
                    results.append({
                        "page_url":   page_url,
                        "link_url":   link_url,
                        "link_text":  link_text,
                        "link_type":  link_type,
                        "status":     lnk_status,
                        "final_url":  lnk_final,
                        "depth":      depth,
                        "effort":     effort_level(lnk_status),
                        "category":   status_category(lnk_status),
                        "timestamp":  datetime.utcnow().isoformat(timespec="seconds"),
                    })
                    # Queue internal pages for crawling
                    if (link_type == "Internal"
                            and str(lnk_status) in ("200", "301", "302")
                            and link_url not in visited_pages
                            and not should_skip_parse(link_url)):
                        queue.append((link_url, depth + 1))

                await asyncio.gather(*[check_one(lu, lt) for lu, lt in page_links])

            await asyncio.gather(*[process_page(u, d) for u, d in batch])

            if CONFIG["POLITE_DELAY"] > 0:
                await asyncio.sleep(CONFIG["POLITE_DELAY"])

    log.info("Crawl complete. %d pages crawled, %d link records collected.",
             len(visited_pages), len(results))
    return results


# ──────────────────────────────────────────────
#  CSV OUTPUT
# ──────────────────────────────────────────────

FIELDS = ["page_url","link_url","link_text","link_type",
          "status","final_url","depth","effort","category","timestamp"]

def write_csv(results: list[dict], path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    log.info("CSV written → %s  (%d rows)", path, len(results))


# ──────────────────────────────────────────────
#  HTML REPORT GENERATION
#  Architecture: all data stored as a JS JSON array.
#  Only PAGE_SIZE rows are rendered into the DOM at once.
#  Filtering/sorting runs entirely on the JS array — never
#  touches 5000+ DOM nodes — so filtering is instant and
#  the browser never freezes regardless of dataset size.
# ──────────────────────────────────────────────

def build_html_report(results: list[dict], csv_path: str, elapsed: float) -> str:

    PAGE_SIZE     = 100   # rows rendered in DOM at one time

    total_links   = len(results)
    pages_set     = {r["page_url"] for r in results}
    total_pages   = len(pages_set)
    broken        = sum(1 for r in results if str(r["status"]) in ("404","410","451"))
    redirects     = sum(1 for r in results if str(r["status"]).startswith("3"))
    server_errors = sum(1 for r in results if str(r["status"]).startswith("5"))
    ok_links      = sum(1 for r in results if str(r["status"]) == "200")
    errors        = sum(1 for r in results if "Error" in str(r["status"]) or "Timeout" in str(r["status"]))
    external      = sum(1 for r in results if r.get("link_type") == "External")
    internal      = sum(1 for r in results if r.get("link_type") == "Internal")

    # Category counts for doughnut chart
    cat_counts: dict[str, int] = {}
    for r in results:
        cat = r.get("category", "Unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # Top parent pages with most broken/error links for bar chart
    broken_by_page: dict[str, int] = {}
    for r in results:
        s = str(r["status"])
        if s in ("404","410","451") or "Error" in s or "Timeout" in s:
            broken_by_page[r["page_url"]] = broken_by_page.get(r["page_url"], 0) + 1
    top_broken = sorted(broken_by_page.items(), key=lambda x: -x[1])[:10]

    chart_labels = json.dumps(list(cat_counts.keys()))
    chart_values = json.dumps(list(cat_counts.values()))
    chart_colors = json.dumps([
        "#22c55e","#16a34a","#f59e0b","#ef4444","#dc2626",
        "#3b82f6","#8b5cf6","#64748b","#0ea5e9","#f97316"
    ][:len(cat_counts)])
    bar_labels = json.dumps([p[:60] + "…" if len(p) > 60 else p for p, _ in top_broken])
    bar_values = json.dumps([v for _, v in top_broken])

    run_time = f"{elapsed:.1f}s"
    run_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    base_url = CONFIG["BASE_URL"]

    def row_class(status):
        s = str(status)
        if s == "200":         return "ok"
        if s.startswith("3"):  return "redirect"
        if s in ("404","410"): return "broken"
        if s.startswith("4"):  return "warn"
        if s.startswith("5"):  return "servererr"
        return "unkn"

    # Serialise all results as a compact JS array — this is the single
    # source of truth; the table is rendered from this, never from DOM.
    js_rows = []
    for r in results:
        js_rows.append({
            "pu":  r["page_url"],               # parent url
            "lu":  r["link_url"],               # link url
            "lt":  r.get("link_text","")[:120], # anchor text
            "tp":  r.get("link_type",""),        # Internal / External
            "st":  str(r["status"]),            # http status
            "fu":  r.get("final_url",""),        # final url after redirect
            "dp":  r.get("depth",""),            # crawl depth
            "ef":  r.get("effort",""),           # effort level
            "ca":  r.get("category",""),         # category label
            "rc":  row_class(r["status"]),       # css row class
            "ts":  str(r.get("timestamp",""))[:19],
        })
    all_data_json = json.dumps(js_rows, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Broken Link Report — {base_url}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg:#0f172a; --surface:#1e293b; --surface2:#263348;
    --border:#334155; --text:#e2e8f0; --muted:#94a3b8;
    --green:#22c55e; --yellow:#f59e0b; --red:#ef4444;
    --blue:#3b82f6; --purple:#8b5cf6; --orange:#f97316;
    --radius:10px;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:14px}}
  a{{color:#60a5fa;text-decoration:none}} a:hover{{text-decoration:underline}}

  .header{{background:var(--surface);border-bottom:1px solid var(--border);padding:18px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}
  .header h1{{font-size:1.3rem;font-weight:700}} .header h1 span{{color:var(--blue)}}
  .meta{{color:var(--muted);font-size:12px;display:flex;gap:16px;flex-wrap:wrap}} .meta b{{color:var(--text)}}
  .hdr-btns{{display:flex;gap:8px}}

  .container{{max-width:1600px;margin:0 auto;padding:20px 28px}}

  .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:24px}}
  .card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;cursor:pointer;transition:border-color .15s}}
  .card:hover{{border-color:var(--blue)}}
  .card.active{{border-color:var(--blue);background:var(--surface2)}}
  .card .val{{font-size:1.9rem;font-weight:800;line-height:1;margin-bottom:4px}}
  .card .lbl{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em}}
  .c-green .val{{color:var(--green)}} .c-red .val{{color:var(--red)}}
  .c-yellow .val{{color:var(--yellow)}} .c-blue .val{{color:var(--blue)}}
  .c-orange .val{{color:var(--orange)}} .c-purple .val{{color:var(--purple)}}
  .c-white .val{{color:var(--text)}}

  .charts{{display:grid;grid-template-columns:320px 1fr;gap:18px;margin-bottom:24px}}
  @media(max-width:860px){{.charts{{grid-template-columns:1fr}}}}
  .chart-box{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px}}
  .chart-box h2{{font-size:.8rem;font-weight:600;margin-bottom:14px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em}}
  .chart-box canvas{{max-height:260px}}

  .filters{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 18px;margin-bottom:14px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}}
  .filters label{{color:var(--muted);font-size:12px;margin-right:3px}}
  .filters input,.filters select{{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:6px 11px;border-radius:6px;font-size:13px;outline:none}}
  .filters input{{width:240px}}
  .filters input:focus,.filters select:focus{{border-color:var(--blue)}}
  .info{{margin-left:auto;color:var(--muted);font-size:12px;white-space:nowrap}}

  .btn{{background:var(--blue);color:#fff;border:none;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600}}
  .btn:hover{{background:#2563eb}}
  .btn-ghost{{background:transparent;border:1px solid var(--border);color:var(--muted)}}
  .btn-ghost:hover{{border-color:var(--blue);color:var(--text)}}

  .table-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:auto}}
  table{{width:100%;border-collapse:collapse}}
  thead th{{background:var(--surface2);padding:9px 13px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}}
  thead th:hover{{color:var(--text)}}
  tbody tr{{border-bottom:1px solid var(--border)}}
  tbody tr:hover{{background:var(--surface2)}}
  td{{padding:8px 13px;vertical-align:middle;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}

  .row-redirect{{background:rgba(245,158,11,.04)}}
  .row-broken{{background:rgba(239,68,68,.07)}}
  .row-warn{{background:rgba(249,115,22,.05)}}
  .row-servererr{{background:rgba(139,92,246,.05)}}
  .row-unkn{{background:rgba(148,163,184,.04)}}

  .pill{{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;font-weight:700}}
  .p-ok{{background:rgba(34,197,94,.15);color:#4ade80}}
  .p-redirect{{background:rgba(245,158,11,.15);color:#fbbf24}}
  .p-broken{{background:rgba(239,68,68,.2);color:#f87171}}
  .p-warn{{background:rgba(249,115,22,.15);color:#fb923c}}
  .p-servererr{{background:rgba(139,92,246,.15);color:#a78bfa}}
  .p-unkn{{background:rgba(148,163,184,.15);color:#94a3b8}}

  .tt{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}}
  .tt-internal{{background:rgba(59,130,246,.15);color:#93c5fd}}
  .tt-external{{background:rgba(139,92,246,.15);color:#c4b5fd}}
  .tt-page{{background:rgba(148,163,184,.15);color:#94a3b8}}

  .bdg{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}}
  .b-none{{background:rgba(34,197,94,.1);color:#4ade80}}
  .b-low{{background:rgba(245,158,11,.1);color:#fbbf24}}
  .b-lowmed{{background:rgba(249,115,22,.1);color:#fb923c}}
  .b-med{{background:rgba(139,92,246,.1);color:#a78bfa}}
  .b-high{{background:rgba(239,68,68,.15);color:#f87171}}

  .pagination{{display:flex;align-items:center;gap:8px;padding:12px 16px;border-top:1px solid var(--border);flex-wrap:wrap}}
  .pagination button{{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:5px 12px;border-radius:5px;cursor:pointer;font-size:12px}}
  .pagination button:hover{{border-color:var(--blue)}}
  .pagination button:disabled{{opacity:.35;cursor:not-allowed}}
  .pagination button.active{{background:var(--blue);border-color:var(--blue);color:#fff}}
  .pagination .pginfo{{color:var(--muted);font-size:12px;margin:0 6px}}

  .sort-asc::after{{content:" ▲";opacity:.8;font-size:9px}}
  .sort-desc::after{{content:" ▼";opacity:.8;font-size:9px}}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Broken Link Report — <span>{base_url}</span></h1>
    <div class="meta">
      <span>Run date: <b>{run_date}</b></span>
      <span>Duration: <b>{run_time}</b></span>
      <span>Depth: <b>{CONFIG["MAX_DEPTH"]}</b></span>
      <span>Workers: <b>{CONFIG["CONCURRENCY"]}</b></span>
    </div>
  </div>
  <div class="hdr-btns">
    <button class="btn btn-ghost" onclick="exportCSV()">⬇ Export CSV</button>
  </div>
</div>

<div class="container">

  <!-- SUMMARY CARDS — clicking a card filters the table -->
  <div class="cards">
    <div class="card c-white" onclick="cardFilter('')"         id="card-all">      <div class="val">{total_pages:,}</div><div class="lbl">Parent Pages</div></div>
    <div class="card c-white" onclick="cardFilter('')"         id="card-links">    <div class="val">{total_links:,}</div><div class="lbl">Total Links</div></div>
    <div class="card c-green" onclick="cardFilter('ok')"       id="card-ok">       <div class="val">{ok_links:,}</div>  <div class="lbl">200 OK</div></div>
    <div class="card c-yellow"onclick="cardFilter('redirect')" id="card-redirect"> <div class="val">{redirects:,}</div> <div class="lbl">Redirects (3xx)</div></div>
    <div class="card c-red"   onclick="cardFilter('broken')"   id="card-broken">   <div class="val">{broken:,}</div>   <div class="lbl">Broken (404/410)</div></div>
    <div class="card c-purple"onclick="cardFilter('servererr')"id="card-5xx">      <div class="val">{server_errors:,}</div><div class="lbl">Server Errors (5xx)</div></div>
    <div class="card c-orange"onclick="cardFilter('unkn')"     id="card-err">      <div class="val">{errors:,}</div>   <div class="lbl">Timeout / Error</div></div>
    <div class="card c-blue"  onclick="typeFilter2('internal')"id="card-int">      <div class="val">{internal:,}</div> <div class="lbl">Internal Links</div></div>
    <div class="card c-white" onclick="typeFilter2('external')"id="card-ext">      <div class="val">{external:,}</div> <div class="lbl">External Links</div></div>
  </div>

  <!-- CHARTS -->
  <div class="charts">
    <div class="chart-box">
      <h2>Status Distribution</h2>
      <canvas id="donut"></canvas>
    </div>
    <div class="chart-box">
      <h2>Top Parent Pages by Broken / Error Links</h2>
      <canvas id="bar"></canvas>
    </div>
  </div>

  <!-- FILTERS -->
  <div class="filters">
    <div><label>Search</label>
      <input type="text" id="search" placeholder="Filter by URL or anchor text…" oninput="applyFilters()"></div>
    <div><label>Status</label>
      <select id="statusFilter" onchange="applyFilters()">
        <option value="">All Statuses</option>
        <option value="ok">200 OK</option>
        <option value="redirect">Redirect (3xx)</option>
        <option value="broken">Broken (404/410)</option>
        <option value="warn">4xx Other</option>
        <option value="servererr">5xx Server Error</option>
        <option value="unkn">Timeout / Error</option>
      </select></div>
    <div><label>Type</label>
      <select id="typeFilter" onchange="applyFilters()">
        <option value="">All Types</option>
        <option value="Internal">Internal</option>
        <option value="External">External</option>
      </select></div>
    <div><label>Effort</label>
      <select id="effortFilter" onchange="applyFilters()">
        <option value="">All Efforts</option>
        <option value="High">High</option>
        <option value="Medium">Medium</option>
        <option value="Low">Low</option>
        <option value="None">None</option>
      </select></div>
    <button class="btn btn-ghost" onclick="resetFilters()">Reset</button>
    <span class="info" id="rowInfo"></span>
  </div>

  <!-- TABLE -->
  <div class="table-wrap">
    <table>
      <thead id="thead">
        <tr>
          <th onclick="sortBy(0)">Parent URL</th>
          <th onclick="sortBy(1)">Link URL</th>
          <th onclick="sortBy(2)">Anchor Text</th>
          <th onclick="sortBy(3)">Type</th>
          <th onclick="sortBy(4)">Status</th>
          <th onclick="sortBy(5)">Category</th>
          <th onclick="sortBy(6)">Depth</th>
          <th onclick="sortBy(7)">Effort</th>
          <th onclick="sortBy(8)">Timestamp</th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
    <div class="pagination" id="pager"></div>
  </div>

</div>

<!-- ALL DATA — never touched by DOM operations, only read by JS -->
<script>
const ALL_DATA = {all_data_json};
const PAGE_SIZE = {PAGE_SIZE};

let filtered  = ALL_DATA.slice();
let sortCol   = -1;
let sortAsc   = true;
let curPage   = 0;

// ── CHARTS ──────────────────────────────────────────────────
new Chart(document.getElementById('donut').getContext('2d'), {{
  type:'doughnut',
  data:{{
    labels:{chart_labels},
    datasets:[{{data:{chart_values},backgroundColor:{chart_colors},borderWidth:2,borderColor:'#1e293b'}}]
  }},
  options:{{responsive:true,plugins:{{
    legend:{{position:'bottom',labels:{{color:'#94a3b8',padding:12,font:{{size:12}}}}}},
    tooltip:{{callbacks:{{label:ctx=>` ${{ctx.label}}: ${{ctx.parsed.toLocaleString()}}`}}}}
  }}}}
}});

new Chart(document.getElementById('bar').getContext('2d'), {{
  type:'bar',
  data:{{
    labels:{bar_labels},
    datasets:[{{label:'Broken / Error Links',data:{bar_values},backgroundColor:'rgba(239,68,68,0.7)',borderRadius:4}}]
  }},
  options:{{
    indexAxis:'y',responsive:true,
    plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:ctx=>` ${{ctx.parsed.x}} issues`}}}}}},
    scales:{{
      x:{{grid:{{color:'#334155'}},ticks:{{color:'#94a3b8'}}}},
      y:{{grid:{{display:false}},ticks:{{color:'#94a3b8',font:{{size:11}}}}}}
    }}
  }}
}});

// ── PILL / BADGE HELPERS ─────────────────────────────────────
function statusPill(rc, st) {{
  return `<span class="pill p-${{rc}}">${{st}}</span>`;
}}
function typeBadge(tp) {{
  const c = tp.toLowerCase();
  return `<span class="tt tt-${{c}}">${{tp}}</span>`;
}}
function effortBadge(ef) {{
  const map = {{'None':'b-none','Low':'b-low','Low-Medium':'b-lowmed','Medium':'b-med','High':'b-high'}};
  return `<span class="bdg ${{map[ef]||'b-med'}}">${{ef}}</span>`;
}}
function esc(s) {{
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}
function short(s,n) {{
  return s.length>n ? s.slice(0,n)+'…' : s;
}}

// ── RENDER PAGE ──────────────────────────────────────────────
// Only PAGE_SIZE <tr> nodes are ever in the DOM at once.
function renderPage(page) {{
  curPage = page;
  const start = page * PAGE_SIZE;
  const slice = filtered.slice(start, start + PAGE_SIZE);
  const tbody = document.getElementById('tbody');

  // Build HTML string then set innerHTML once — single reflow
  const html = slice.map(r => `
    <tr class="row-${{r.rc}}">
      <td title="${{esc(r.pu)}}"><a href="${{esc(r.pu)}}" target="_blank">${{esc(short(r.pu,70))}}</a></td>
      <td title="${{esc(r.lu)}}"><a href="${{esc(r.lu)}}" target="_blank">${{esc(short(r.lu,70))}}</a></td>
      <td title="${{esc(r.lt)}}">${{esc(short(r.lt,50))}}</td>
      <td>${{typeBadge(r.tp||'Page')}}</td>
      <td>${{statusPill(r.rc, r.st)}}</td>
      <td>${{esc(r.ca)}}</td>
      <td>${{esc(r.dp)}}</td>
      <td>${{effortBadge(r.ef)}}</td>
      <td>${{esc(r.ts)}}</td>
    </tr>`).join('');
  tbody.innerHTML = html;

  renderPager();
  document.getElementById('rowInfo').textContent =
    `${{filtered.length.toLocaleString()}} rows · page ${{page+1}} of ${{Math.max(1,Math.ceil(filtered.length/PAGE_SIZE))}}`;
}}

// ── PAGINATOR ────────────────────────────────────────────────
function renderPager() {{
  const total   = Math.ceil(filtered.length / PAGE_SIZE);
  const pager   = document.getElementById('pager');
  if (total <= 1) {{ pager.innerHTML = ''; return; }}

  const maxBtns = 9;
  let pages = [];
  if (total <= maxBtns) {{
    pages = Array.from({{length:total}},(_,i)=>i);
  }} else {{
    pages = [0];
    let lo = Math.max(1, curPage-3), hi = Math.min(total-2, curPage+3);
    if (lo > 1)       pages.push('…');
    for (let i=lo;i<=hi;i++) pages.push(i);
    if (hi < total-2) pages.push('…');
    pages.push(total-1);
  }}

  pager.innerHTML =
    `<button onclick="renderPage(${{curPage-1}})" ${{curPage===0?'disabled':''}}>‹ Prev</button>` +
    pages.map(p => p==='…'
      ? `<span class="pginfo">…</span>`
      : `<button class="${{p===curPage?'active':''}}" onclick="renderPage(${{p}})">${{p+1}}</button>`
    ).join('') +
    `<button onclick="renderPage(${{curPage+1}})" ${{curPage>=total-1?'disabled':''}}>Next ›</button>`;
}}

// ── FILTER ───────────────────────────────────────────────────
let searchTimer = null;

function applyFilters() {{
  clearTimeout(searchTimer);
  searchTimer = setTimeout(_applyFilters, 120);  // debounce search input
}}

function _applyFilters() {{
  const q  = document.getElementById('search').value.toLowerCase().trim();
  const st = document.getElementById('statusFilter').value;
  const tp = document.getElementById('typeFilter').value;
  const ef = document.getElementById('effortFilter').value;

  filtered = ALL_DATA.filter(r => {{
    if (st && r.rc  !== st)  return false;
    if (tp && r.tp  !== tp)  return false;
    if (ef && r.ef  !== ef)  return false;
    if (q  && !( r.pu.toLowerCase().includes(q) ||
                 r.lu.toLowerCase().includes(q) ||
                 r.lt.toLowerCase().includes(q) )) return false;
    return true;
  }});

  if (sortCol >= 0) sortFiltered();
  renderPage(0);
  updateCardHighlight();
}}

function resetFilters() {{
  document.getElementById('search').value   = '';
  document.getElementById('statusFilter').value = '';
  document.getElementById('typeFilter').value   = '';
  document.getElementById('effortFilter').value  = '';
  filtered  = ALL_DATA.slice();
  sortCol   = -1;
  sortAsc   = true;
  document.querySelectorAll('#thead th').forEach(th=>{{
    th.classList.remove('sort-asc','sort-desc');
  }});
  document.querySelectorAll('.card').forEach(c=>c.classList.remove('active'));
  renderPage(0);
}}

// ── CARD QUICK FILTERS ───────────────────────────────────────
function cardFilter(rc) {{
  document.getElementById('statusFilter').value = rc;
  document.getElementById('typeFilter').value   = '';
  document.getElementById('effortFilter').value  = '';
  document.getElementById('search').value        = '';
  _applyFilters();
}}
function typeFilter2(tp) {{
  document.getElementById('typeFilter').value   = tp.charAt(0).toUpperCase()+tp.slice(1);
  document.getElementById('statusFilter').value = '';
  document.getElementById('effortFilter').value  = '';
  document.getElementById('search').value        = '';
  _applyFilters();
}}
function updateCardHighlight() {{
  const st = document.getElementById('statusFilter').value;
  const tp = document.getElementById('typeFilter').value.toLowerCase();
  document.querySelectorAll('.card').forEach(c=>c.classList.remove('active'));
  const map = {{'ok':'card-ok','redirect':'card-redirect','broken':'card-broken',
                'servererr':'card-5xx','unkn':'card-err'}};
  if (map[st])          document.getElementById(map[st])?.classList.add('active');
  if (tp==='internal')  document.getElementById('card-int')?.classList.add('active');
  if (tp==='external')  document.getElementById('card-ext')?.classList.add('active');
}}

// ── SORT ─────────────────────────────────────────────────────
const SORT_KEYS = ['pu','lu','lt','tp','st','ca','dp','ef','ts'];

function sortBy(col) {{
  if (sortCol === col) {{ sortAsc = !sortAsc; }}
  else {{ sortCol = col; sortAsc = true; }}
  document.querySelectorAll('#thead th').forEach((th,i) => {{
    th.classList.remove('sort-asc','sort-desc');
    if (i===col) th.classList.add(sortAsc?'sort-asc':'sort-desc');
  }});
  sortFiltered();
  renderPage(0);
}}

function sortFiltered() {{
  const key = SORT_KEYS[sortCol];
  filtered.sort((a,b) => {{
    const va = String(a[key]??''), vb = String(b[key]??'');
    const n  = Number(va) - Number(vb);
    const cmp = isNaN(n) ? va.localeCompare(vb) : n;
    return sortAsc ? cmp : -cmp;
  }});
}}

// ── CSV EXPORT (exports filtered data, not just current page) ─
function exportCSV() {{
  const headers = ['Parent URL','Link URL','Anchor Text','Type','Status',
                   'Category','Depth','Effort','Timestamp'];
  const keys    = ['pu','lu','lt','tp','st','ca','dp','ef','ts'];
  const lines   = [headers.join(',')];
  filtered.forEach(r => {{
    lines.push(keys.map(k => '"' + String(r[k]??'').replace(/"/g,'""') + '"').join(','));
  }});
  const blob = new Blob([lines.join('\\n')], {{type:'text/csv'}});
  const a = Object.assign(document.createElement('a'),
    {{href:URL.createObjectURL(blob), download:'broken_links_filtered.csv'}});
  a.click();
}}

// ── INIT ─────────────────────────────────────────────────────
renderPage(0);
</script>
</body>
</html>"""
    return html


# ──────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────

async def main():
    start_time = time.time()
    log.info("Starting crawl of %s", CONFIG["BASE_URL"])
    log.info("Config: MAX_DEPTH=%d  MAX_PAGES=%d  CONCURRENCY=%d  TIMEOUT=%ds",
             CONFIG["MAX_DEPTH"], CONFIG["MAX_PAGES"],
             CONFIG["CONCURRENCY"], CONFIG["TIMEOUT"])

    results = await crawl()

    if not results:
        log.warning("No results collected. Check BASE_URL and network access.")
        return

    elapsed = time.time() - start_time

    output_dir = Path(CONFIG["OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)

    ts_tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path  = str(output_dir / f"broken_links_{ts_tag}.csv")
    html_path = str(output_dir / f"report_{ts_tag}.html")

    write_csv(results, csv_path)

    html = build_html_report(results, csv_path, elapsed)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info("HTML report written → %s", html_path)

    # Also write index.html — GitHub Pages always serves this as the latest report
    index_path = str(output_dir / "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info("index.html written → %s (used by GitHub Pages)", index_path)

    # Summary to stdout (picked up by GitHub Actions step summary)
    broken = sum(1 for r in results if str(r["status"]) in ("404","410","451"))
    ok     = sum(1 for r in results if str(r["status"]) == "200")
    print(f"\n{'─'*60}")
    print(f"  CRAWL SUMMARY")
    print(f"  Base URL   : {CONFIG['BASE_URL']}")
    print(f"  Pages      : {len({r['page_url'] for r in results}):,}")
    print(f"  Total links: {len(results):,}")
    print(f"  200 OK     : {ok:,}")
    print(f"  Broken (404/410): {broken:,}")
    print(f"  Duration   : {elapsed:.1f}s")
    print(f"  Reports    : {csv_path}")
    print(f"              {html_path}")
    print(f"{'─'*60}\n")

    # GitHub Actions step summary
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(f"## Crawl Results for `{CONFIG['BASE_URL']}`\n\n")
            f.write(f"| Metric | Value |\n|---|---|\n")
            f.write(f"| Pages Crawled | {len({r['page_url'] for r in results}):,} |\n")
            f.write(f"| Total Links | {len(results):,} |\n")
            f.write(f"| 200 OK | {ok:,} |\n")
            f.write(f"| Broken (404/410) | {broken:,} |\n")
            f.write(f"| Duration | {elapsed:.1f}s |\n")


if __name__ == "__main__":
    asyncio.run(main())
