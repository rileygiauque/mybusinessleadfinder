import os, re, time
from datetime import date, timedelta
from typing import List, Dict, Iterable
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt
from urllib.parse import urljoin
from concurrent.futures import ProcessPoolExecutor, as_completed
import random



BS_PARSER = os.getenv("NBP_BS_PARSER", "lxml")
DATE_DOC_RX = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}\s+--\s+', re.I)



HOME = "https://search.sunbiz.org"
BYNAME = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
USER_AGENT = os.getenv("NBP_UA", "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36")
DEBUG = os.getenv("NBP_DEBUG", "0") == "1"

# tuneables
MAX_DETAIL_PER_PREFIX = int(os.getenv("NBP_MAX_DETAIL_PER_PREFIX", "0"))  # hard cap to stay polite
SLEEP_MS = int(os.getenv("NBP_SLEEP_MS", "600"))  # between page actions

STATUS_RX = os.getenv("NBP_STATUS_REGEX", r"^\s*active\b")  # matches “Active”, case-insensitive



def _status_ok(s: str) -> bool:
    return bool(re.search(STATUS_RX, (s or ""), flags=re.I))

def _norm(s: str) -> str:
    return re.sub(r'[^A-Z0-9]', '', (s or '').upper())

def _matches_prefix(name: str, prefix: str) -> bool:
    return _norm(name).startswith(_norm(prefix))

# jittered sleep to avoid bursty patterns
JITTER_MS = int(os.getenv("NBP_JITTER_MS", "0"))

def _sleep(ms=None):
    if ms is None:
        ms = SLEEP_MS
    if JITTER_MS > 0:
        ms = ms + random.randint(0, JITTER_MS)
    time.sleep(ms / 1000)


def _save_debug(name: str, html: str):
    if not DEBUG: return
    with open(f"debug_{name}.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[sunbiz] wrote debug_", name)

def _parse_results_table(html: str) -> List[Dict]:
    """
    Parse the 'Entity Name List' table rows into {'name','doc','status','href'} records.
    """
    soup = BeautifulSoup(html, BS_PARSER)
    table = None
    # Find the table that contains headers like Corporate Name / Document Number / Status
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True).lower()
        if "document number" in txt and "status" in txt:
            table = t; break
    if not table:
        return []

    out = []
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if ths:  # header row
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        name_cell = tds[0]
        name = name_cell.get_text(" ", strip=True)
        a = name_cell.find("a")
        href = a.get("href") if a else None
        doc = tds[1].get_text(" ", strip=True)
        status = tds[2].get_text(" ", strip=True)
        if name and doc and href:
            out.append({"name": name, "doc": doc, "status": status, "href": href})
    return out

# --- replace _parse_detail with this ---
def _parse_detail(html: str) -> Dict:
    """
    Parse a Sunbiz detail page and return normalized fields.
    Captures:
      - filing_date, effective_date
      - entity_type
      - fei_ein
      - last_event, event_date_filed, event_effective_date
      - registered_agent_name, registered_agent_address
      - principal_address, mailing_address
      - city (best-effort from addresses)
      - officers/authorized persons: list of {title, name, address}
      - status
    """
    if not html:
        return {"filing_date": date.today()}

    parser = os.getenv("NBP_BS_PARSER", "lxml")
    soup = BeautifulSoup(html, parser)
    text = soup.get_text("\n", strip=True)

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # headings we never want inside officers/persons
    NOISE_RX = re.compile(
        r'^(Annual Reports|No Annual Reports Filed|Document Images|View image in PDF format|'
        r'Previous On List|Next On List|Return to List)\b',
        re.I
    )

    # common “entity type” phrases
    TYPE_RX = re.compile(
        r'\b(Florida|Foreign)\s+('
        r'Limited Liability Company|Profit Corporation|Not For Profit Corporation|'
        r'Limited Partnership|Limited Liability Limited Partnership|'
        r'Limited Liability Partnership|Professional Corporation|'
        r'Professional Limited Liability Company|General Partnership|Association'
        r')\b',
        re.I
    )   


    def _clean_lines(block: str, max_lines: int = 12) -> str | None:
        lines = [ln.strip(" \t\r\n:") for ln in (block or "").splitlines() if ln.strip()]
        return ", ".join(lines[:max_lines]) if lines else None

    def _block_between(label: str, stops: list[str], max_chars: int = 4000) -> str | None:
        rx = re.compile(rf"{re.escape(label)}\s*\n(.+?)\n(?:{'|'.join(map(re.escape, stops))})", re.I | re.S)
        m = rx.search(text)
        if not m:
            rx2 = re.compile(rf"{re.escape(label)}\s*\n(.+)$", re.I | re.S)
            m = rx2.search(text)
        return _clean_lines((m.group(1)[:max_chars] if m else None))

    # dates
    filing_date = None
    m = re.search(r"(Date Filed|Filed On)\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
    if m:
        try: filing_date = parse_dt(m.group(2)).date()
        except: pass

    effective_date = None
    m = re.search(r"Effective Date\s*:?\s*(\d{1,2}/\d{1,2}/\d{4}|NONE)", text, re.I)
    if m and m.group(1).upper() != "NONE":
        try: effective_date = parse_dt(m.group(1)).date()
        except: pass

    # ---------- Entity Type ----------
    entity_type = None

    # Prefer the header area: the line(s) immediately under "Detail by Entity Name"
    try:
        i = next(idx for idx, ln in enumerate(lines)
                 if ln.lower().startswith("detail by entity name"))
        for cand in lines[i+1:i+5]:          # look at the next few non-empty lines
            if TYPE_RX.search(cand):
                entity_type = cand.strip()
                break
    except StopIteration:
        pass

    # Fallback A: explicit "Entity Type:" label when present
    if not entity_type:
        m = re.search(r"\bEntity Type\b\s*:?\s*([A-Za-z][A-Za-z &/\-]{2,80})", text, re.I)
        if m:
            entity_type = m.group(1).strip()

    # Fallback B: anywhere in page text
    if not entity_type:
        m = TYPE_RX.search(text)
        if m:
            entity_type = f"{m.group(1)} {m.group(2)}".strip()

    if entity_type:
        entity_type = entity_type[:50]
    
    # FEI/EIN
    fei_ein = None
    m = re.search(r"FEI/EIN Number\s*:?\s*([A-Z0-9\- ]+|APPLIED FOR|NONE)", text, re.I)
    if m: fei_ein = m.group(1).strip()

    # last event + event dates
    last_event = None
    m = re.search(r"Last Event\s*:?\s*([^\n]+)", text, re.I)
    if m: last_event = m.group(1).strip()

    event_date_filed = None
    m = re.search(r"Event Date Filed\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
    if m:
        try: event_date_filed = parse_dt(m.group(1)).date()
        except: pass

    event_effective_date = None
    m = re.search(r"Event Effective Date\s*:?\s*(\d{1,2}/\d{1,2}/\d{4}|NONE)", text, re.I)
    if m and m.group(1).upper() != "NONE":
        try: event_effective_date = parse_dt(m.group(1)).date()
        except: pass

    stops = [
        "Mailing Address",
        "Registered Agent",
        "Registered Agent Name",
        "Registered Agent Name & Address",
        "Filing Information",
        "FEI/EIN Number",
        "Officer/Director Detail",
        "Authorized Person(s) Detail",
        "Annual Reports",
        "No Annual Reports Filed",
        "Document Images",
        "View image in PDF format",
        "No Name History",
        "No Events",
        "Previous On List",
        "Next On List",
        "Return to List",
        "Name and Address",
        "Status",
        "Last Event",
        "Event Date Filed",
        "Event Effective Date",
    ]


    principal_address = _block_between("Principal Address", stops)
    mailing_address   = _block_between("Mailing Address",   stops)

    # Registered Agent name + address (robust line-based parse)
    ra_name = None
    ra_addr = None

    # Prefer the combined "Registered Agent Name & Address" section
    ra_raw = re.search(
        rf"Registered Agent Name\s*&\s*Address\s*\n(.+?)(?:\n(?:{'|'.join(map(re.escape, stops))})|$)",
        text, re.I | re.S
    )
    if ra_raw:
        ra_lines = [ln.strip() for ln in ra_raw.group(1).splitlines() if ln.strip()]
        if ra_lines:
            ra_name = ra_lines[0]                  # full first line = name
            if len(ra_lines) > 1:
                ra_addr = ", ".join(ra_lines[1:])  # remaining lines = address
    else:
        # Fallbacks when combined block isn't present
        m = re.search(r"Registered Agent(?: Name(?: & Address)?)?\s*\n([^\n]+)", text, re.I)
        if m:
            ra_name = m.group(1).strip()

        # Sometimes address is a separate block
        ra_addr_block = re.search(
            rf"Registered Agent Address\s*\n(.+?)(?:\n(?:{'|'.join(map(re.escape, stops))})|$)",
            text, re.I | re.S
        )
        if ra_addr_block:
            ra_addr_lines = [ln.strip() for ln in ra_addr_block.group(1).splitlines() if ln.strip()]
            if ra_addr_lines:
                ra_addr = ", ".join(ra_addr_lines)

    # City best-effort
    city = None
    search_src = principal_address or mailing_address or text
    m = re.search(r"\b([A-Z][A-Za-z .'\-]+),\s*FL\b", search_src, re.I)
    if m:
        city = m.group(1).strip().title()

    # Status
    status = None
    m = re.search(r"\bStatus\b\s*:?\s*([A-Za-z /\-]+)", text, re.I)
    if m: status = m.group(1).strip()

    # Officers / Authorized Persons
    def _parse_people(section_label: str) -> list[dict]:
        blk = _block_between(section_label, stops, max_chars=8000)
        if not blk:
            return []
        raw = re.search(
            rf"{re.escape(section_label)}\s*\n(.+?)(?:\n(?:{'|'.join(map(re.escape, stops))})|$)",
            text, re.I | re.S
        )

        lines = []
        if raw:
            for ln in raw.group(1).splitlines():
                t = ln.strip()
                if not t:
                    continue
                # unified noise filter
                if NOISE_RX.search(t) or DATE_DOC_RX.search(t):
                    continue
                lines.append(t)

        people = []
        i = 0
        while i < len(lines):
            if lines[i].lower().startswith("title"):
                title = lines[i].split(None, 1)[1].strip() if " " in lines[i] else lines[i].strip()
                i += 1
                # next non-title line = name
                name = None
                addr_lines = []
                while i < len(lines) and not lines[i].lower().startswith("title"):
                    if name is None:
                        name = lines[i]
                    else:
                        addr_lines.append(lines[i])
                    i += 1
                people.append({
                    "title": title,
                    "name": name,
                    "address": ", ".join(addr_lines) if addr_lines else None
                })
            else:
                i += 1
        return people

    officers = _parse_people("Officer/Director Detail")
    if not officers:
        officers = _parse_people("Authorized Person(s) Detail")

    return {
        "filing_date": filing_date or date.today(),
        "effective_date": effective_date,
        "entity_type": entity_type,
        "fei_ein": fei_ein,
        "last_event": last_event,
        "event_date_filed": event_date_filed,
        "event_effective_date": event_effective_date,
        "registered_agent_name": ra_name,
        "registered_agent_address": ra_addr,
        "principal_address": principal_address,
        "mailing_address": mailing_address,
        "city": city,
        "county": None,
        "status": status,
        "officers": officers,
    }

def fetch_recent_by_name_prefixes(*, window_days: int = None, prefixes: Iterable[str]) -> List[Dict]:
    """Run one Playwright browser per prefix in parallel (process pool)."""
    if window_days is None:
        window_days = int(os.getenv("NBP_WINDOW_DAYS", "90"))

    prefixes = list(prefixes)
    max_workers = int(os.getenv("NBP_CONCURRENCY", "8"))  # be polite

    results: List[Dict] = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_crawl_one_prefix, pref, window_days) for pref in prefixes]
        for fut in as_completed(futures):
            results.extend(fut.result())
    return results

# --- Parallel wrapper --------------------------------------------------------
def fetch_recent_by_name_prefixes_parallel(*, window_days: int, prefixes: Iterable[str], concurrency: int = 8) -> List[Dict]:
    """
    Run fetch_recent_by_name_prefixes over multiple workers in parallel.
    Each worker handles a slice of prefixes. Uses threads; each worker
    launches its own Playwright session.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    prefixes = list(prefixes)
    if concurrency < 1:
        concurrency = 1
    if concurrency > len(prefixes):
        concurrency = len(prefixes)

    # split prefixes into N buckets via round-robin so each worker gets spread out
    buckets = [prefixes[i::concurrency] for i in range(concurrency)]

    results: List[Dict] = []

    def _worker(batch: List[str]) -> List[Dict]:
        if not batch:
            return []
        # reuse the single-worker crawler you already have
        return fetch_recent_by_name_prefixes(window_days=window_days, prefixes=batch)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(_worker, b) for b in buckets]
        for fut in as_completed(futs):
            try:
                results.extend(fut.result())
            except Exception as e:
                print("[sunbiz] worker error:", e)

    return results



def _crawl_one_prefix(prefix: str, window_days: int) -> List[Dict]:
    """Crawl ALL pages for one prefix and return kept rows."""
    today = date.today()
    if window_days is None:
        window_days = int(os.getenv("NBP_WINDOW_DAYS", "90"))

    # Use the larger window: last N days OR year-to-date (whichever starts earlier)
    year_start   = date(today.year, 1, 1)
    ninety_start = today - timedelta(days=window_days)
    window_start = min(year_start, ninety_start)

    def in_window(d):
        return d is not None and window_start <= d <= today


    keep: List[Dict] = []
    PER_PAGE_CAP = int(os.getenv("NBP_MAX_DETAIL_PER_PREFIX", "0"))  # 0 = unlimited
    MAX_PAGES    = int(os.getenv("NBP_MAX_PAGES_PER_PREFIX", "0"))    # 0 = unlimited

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(os.getenv("NBP_HEADLESS", "1") == "1"))
        ctx = browser.new_context(user_agent=USER_AGENT, java_script_enabled=True, locale="en-US")
        page = ctx.new_page()

        # warm-up + search
        page.goto(HOME, wait_until="networkidle", timeout=60000); _sleep()
        page.goto(BYNAME, wait_until="domcontentloaded", timeout=60000); _sleep()
        box = page.query_selector('input[name*="SearchTerm" i], input[type="text"]')
        if not box: browser.close(); return keep
        box.fill(prefix); _sleep(200)
        btn = page.query_selector('button:has-text("Search"), input[type="submit"]')
        if not btn: browser.close(); return keep
        btn.click()
        try: page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout: page.wait_for_load_state("domcontentloaded", timeout=15000)
        _sleep()

        pages_seen = 0
        while True:
            if MAX_PAGES and pages_seen >= MAX_PAGES:
                break

            html = page.content()
            rows_all = _parse_results_table(html)
            if not rows_all:
                break

            pref_norm = _norm(prefix)

            # 1) Detect prefix boundary using ALL rows (Active + Inactive)
            pref_rows_all = [r for r in rows_all if _matches_prefix(r.get("name", ""), pref_norm)]

            # If we’ve already paged at least once and the current page has NO rows
            # with our prefix, Sunbiz rolled past our prefix → stop this prefix.
            if pages_seen > 0 and not pref_rows_all:
                print(f"[sunbiz][{prefix}] prefix rolled off at page {pages_seen+1}; stopping.")
                break

            # 2) Only CLICK details for Active rows within that prefix
            active_pref_rows = [r for r in pref_rows_all if _status_ok(r.get("status"))]

            print(
                f"[sunbiz][{prefix}] page {pages_seen+1}: "
                f"total={len(rows_all)} pref={len(pref_rows_all)} active_pref={len(active_pref_rows)}"
            )

            rows_iter = active_pref_rows if PER_PAGE_CAP == 0 else active_pref_rows[:PER_PAGE_CAP]



            for row in rows_iter:
                detail_url = row["href"]
                if not detail_url.startswith("http"):
                    detail_url = urljoin(HOME, detail_url)

                # robust nav to detail
                nav_ok = False
                for attempt in range(2):
                    try:
                        page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                        nav_ok = True
                        break
                    except Exception:
                        # fall back to clicking a link if direct nav gets aborted
                        try:
                            # try clicking a link that matches the document number or the name
                            candidate = (
                                page.locator("a", has_text=(row.get("doc") or "")).first
                                if row.get("doc") else page.locator("table a", has_text=(row.get("name") or "")).first
                            )
                            if candidate and candidate.count():
                                candidate.click()
                                page.wait_for_load_state("domcontentloaded", timeout=60000)
                                nav_ok = True
                                break
                        except Exception:
                            pass
                if not nav_ok:
                    _save_debug(f"detail_nav_err_{row.get('doc','unknown')}", page.content())
                    continue

                _sleep(300)
                dhtml = page.content()

                info = _parse_detail(page.content())

                # go back to list BEFORE next item or page turn
                page.go_back(wait_until="domcontentloaded", timeout=60000)
                _sleep(200)

                if info.get("status") and not _status_ok(info["status"]):
                    continue

                # keep if Date Filed OR Event Date Filed is in window
                if any(in_window(info.get(k)) for k in ("filing_date", "event_date_filed")):
                    keep.append({
                        "name": row["name"][:255],
                        "doc_number": row["doc"][:100],
                        "entity_type": (info.get("entity_type") or None),
                        "filing_date": info.get("filing_date"),
                        "effective_date": info.get("effective_date"),
                        "fei_ein": info.get("fei_ein"),
                        "last_event": info.get("last_event"),
                        "event_date_filed": info.get("event_date_filed"),
                        "event_effective_date": info.get("event_effective_date"),
                        "registered_agent": info.get("registered_agent_name"),
                        "registered_agent_address": info.get("registered_agent_address"),
                        "principal_address": info.get("principal_address"),
                        "mailing_address": info.get("mailing_address"),
                        "city": info.get("city"),
                        "county": None,
                        "officers": info.get("officers") or [],
                        "status": info.get("status"),
                    })

                    cap = int(os.getenv("NBP_TARGET_TOTAL", "0"))
                    if cap and len(keep) >= cap:
                        browser.close()
                        return keep

            # Next results page for SAME prefix
            next_loc = page.locator("a", has_text=re.compile(r"^\s*Next List\s*$", re.I)).first
            if not next_loc.count():
                next_loc = page.locator("a", has_text=re.compile(r"^\s*Next>", re.I)).first
            if next_loc.count():
                next_loc.click()
                try: page.wait_for_load_state("networkidle", timeout=15000)
                except PWTimeout: page.wait_for_load_state("domcontentloaded", timeout=15000)
                _sleep()
                pages_seen += 1
            else:
                break

        browser.close()
    return keep



def fetch_new_by_name_prefixes(target_dates, prefixes: Iterable[str]) -> List[Dict]:
    """
    For each prefix, iterate ALL search-result pages and keep detail pages that
    match your date criteria. Respects env caps:
      - NBP_TARGET_TOTAL           (0 = no overall cap)
      - NBP_MAX_DETAIL_PER_PREFIX  (0 = no per-page cap)
      - NBP_MAX_PAGES_PER_PREFIX   (0 = no page cap)
    """
    if isinstance(target_dates, date):
        target_dates = {target_dates}

    keep: List[Dict] = []
    CAP_TOTAL     = int(os.getenv("NBP_TARGET_TOTAL", "0"))            # 0 = unlimited
    PER_PAGE_CAP  = int(os.getenv("NBP_MAX_DETAIL_PER_PREFIX", "0"))  # 0 = unlimited
    MAX_PAGES     = int(os.getenv("NBP_MAX_PAGES_PER_PREFIX", "0"))    # 0 = unlimited
    NON_MATCH_STREAK_LIMIT = int(os.getenv("NBP_NON_MATCH_STREAK", os.getenv("NBP_NON_TODAY_STREAK", "0")))  # 0 = no early stop

    def _open_detail_from_row(page, row) -> bool:
        href = (row.get("href") or "").strip()
        name = (row.get("name") or "").strip()
        # 1) direct navigate when possible
        if href and not href.lower().startswith("javascript"):
            try:
                page.goto(urljoin(HOME, href), wait_until="domcontentloaded", timeout=60000)
                return True
            except Exception:
                pass
        # 2) click the name link
        try:
            link = page.locator("table a", has_text=name).first
            if link.count():
                link.click()
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                return True
        except Exception:
            pass
        # 3) click by row/doc number
        try:
            rowloc = page.locator("table tr", has_text=row.get("doc","")).first
            if rowloc.count():
                rowloc.locator("a").first.click()
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                return True
        except Exception:
            pass
        return False


    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(os.getenv("NBP_HEADLESS", "1") == "1"))
        ctx = browser.new_context(user_agent=USER_AGENT, java_script_enabled=True, locale="en-US")
        page = ctx.new_page()

        # Warm-up
        page.goto(HOME, wait_until="networkidle", timeout=60000)
        _sleep()

        for pref in prefixes:
            if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                break

            # Search for prefix
            page.goto(BYNAME, wait_until="domcontentloaded", timeout=60000)
            _sleep()
            box = page.query_selector('input[name*="SearchTerm" i], input[type="text"]')
            if not box:
                _save_debug(f"byname_form_missing_{pref}", page.content())
                continue
            box.fill(pref)
            _sleep(200)

            btn = page.query_selector('button:has-text("Search"), input[type="submit"]')
            if not btn:
                _save_debug(f"byname_no_submit_{pref}", page.content())
                continue
            btn.click()
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PWTimeout:
                page.wait_for_load_state("domcontentloaded", timeout=15000)
            _sleep()

            # Paginate through results
            pages_seen = 0
            while True:
                if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                    break
                if MAX_PAGES and pages_seen >= MAX_PAGES:
                    break

                html = page.content()
                _save_debug(f"results_{pref}_p{pages_seen+1}", html)
                rows_all = _parse_results_table(html)
                if not rows_all:
                    break

                pref_norm = _norm(pref)

                # Detect prefix boundary using ALL rows
                pref_rows_all = [r for r in rows_all if _matches_prefix(r.get("name", ""), pref_norm)]
                if pages_seen > 0 and not pref_rows_all:
                    print(f"[sunbiz][{pref}] prefix rolled off at page {pages_seen+1}; stopping.")
                    break

                # Only click Active rows within the prefix
                active_pref_rows = [r for r in pref_rows_all if _status_ok(r.get("status"))]

                print(
                    f"[sunbiz][{pref}] page {pages_seen+1}: "
                    f"total={len(rows_all)} pref={len(pref_rows_all)} active_pref={len(active_pref_rows)}"
                )

                rows_iter = active_pref_rows if PER_PAGE_CAP == 0 else active_pref_rows[:PER_PAGE_CAP]


                non_match_streak = 0

                for row in rows_iter:
                    if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                        break

                    if not _open_detail_from_row(page, row):
                        _save_debug(f"detail_open_failed_{row.get('doc','unknown')}", page.content())
                        continue

                    _sleep(300)
                    dhtml = page.content()
                    if DEBUG:
                        _save_debug(f"detail_{row['doc']}", dhtml)

                    try:
                        info = _parse_detail(dhtml)
                    except Exception:
                        _save_debug(f"detail_parse_err_{row['doc']}", dhtml)
                        page.go_back(wait_until="domcontentloaded", timeout=60000)
                        _sleep(200)
                        continue

                    if info.get("status") and not _status_ok(info["status"]):
                        page.go_back(wait_until="domcontentloaded", timeout=60000)
                        _sleep(200)
                        continue

                    if info.get("filing_date") in target_dates:
                        keep.append({
                            "name": row["name"][:255],
                            "doc_number": row["doc"][:100],
                            "entity_type": (info.get("entity_type") or None),
                            "filing_date": info.get("filing_date"),
                            "effective_date": info.get("effective_date"),
                            "fei_ein": info.get("fei_ein"),
                            "last_event": info.get("last_event"),
                            "event_date_filed": info.get("event_date_filed"),
                            "event_effective_date": info.get("event_effective_date"),
                            "registered_agent": info.get("registered_agent_name"),
                            "registered_agent_address": info.get("registered_agent_address"),
                            "principal_address": info.get("principal_address"),
                            "mailing_address": info.get("mailing_address"),
                            "city": info.get("city"),
                            "county": None,
                            "officers": info.get("officers") or [],
                        })
                        non_match_streak = 0
                    else:
                        non_match_streak += 1

                    # return to results
                    page.go_back(wait_until="domcontentloaded", timeout=60000)
                    _sleep(200)


                # Next results page?
                if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                    break
                next_loc = page.locator("a", has_text=re.compile(r"^(Next List|Next>)$", re.I)).first
                if next_loc.count():
                    next_loc.click()
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except PWTimeout:
                        page.wait_for_load_state("domcontentloaded", timeout=15000)
                    _sleep()
                    pages_seen += 1
                else:
                    break


        browser.close()
    return keep
