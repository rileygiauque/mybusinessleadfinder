import os, re, time
import re
from datetime import date, timedelta
from typing import List, Dict, Iterable
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt

BS_PARSER = os.getenv("NBP_BS_PARSER", "lxml")


HOME = "https://search.sunbiz.org"
BYNAME = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
USER_AGENT = os.getenv("NBP_UA", "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36")
DEBUG = os.getenv("NBP_DEBUG", "0") == "1"

# tuneables
MAX_DETAIL_PER_PREFIX = int(os.getenv("NBP_MAX_DETAIL_PER_PREFIX", "40"))  # hard cap to stay polite
SLEEP_MS = int(os.getenv("NBP_SLEEP_MS", "600"))  # between page actions

STATUS_RX = os.getenv("NBP_STATUS_REGEX", r"^\s*active\b")  # matches “Active”, case-insensitive

def _status_ok(s: str) -> bool:
    return bool(re.search(STATUS_RX, (s or ""), flags=re.I))

def _sleep(ms=SLEEP_MS):
    time.sleep(ms/1000)

def _save_debug(name: str, html: str):
    if not DEBUG: return
    with open(f"debug_{name}.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[sunbiz] wrote debug_", name)

def _parse_results_table(html: str) -> List[Dict]:
    """
    Parse the 'Entity Name List' table rows into {'name','doc','status','href'} records.
    """
    soup = BeautifulSoup(html, "html.parser")
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

    # entity type
    entity_type = None
    m = re.search(r"Entity Type\s*:?\s*([A-Za-z][A-Za-z &/\-]{2,80})", text, re.I)
    if m: entity_type = m.group(1).strip()
    if not entity_type:
        m = re.search(r"Detail by Entity Name\s*\n([^\n]{3,80})", text, re.I)
        if m:
            cand = m.group(1).strip()
            if re.search(r"^Florida\s+", cand, re.I):
                entity_type = cand
    if entity_type: entity_type = entity_type[:50]

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

    # Registered Agent name + address (prefer the combined block)
    ra_name = None
    ra_addr = None
    ra_block = _block_between("Registered Agent Name & Address", stops)
    if ra_block:
        # first line is name, rest is address
        parts = [ln.strip() for ln in ra_block.split(",") if ln.strip()]
        lnz = [ln.strip() for ln in (ra_block or "").split(",")]
        lines = [ln.strip() for ln in (ra_block or "").split(",")]
        first_line = (ra_block.split(",")[0] if "," in ra_block else ra_block).strip()
        ra_name = first_line
        ra_addr = ra_block[len(first_line):].strip(" ,")
    else:
        # older pages: "Registered Agent" followed by a single line name and address lives elsewhere
        m = re.search(r"Registered Agent(?: Name(?: & Address)?)?\s*\n([^\n]+)", text, re.I)
        if m: ra_name = m.group(1).strip()

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
        if not blk: return []
        # reconstruct lines (the earlier _clean_lines joined with commas; rebuild sensibly)
        raw = re.search(rf"{re.escape(section_label)}\s*\n(.+?)(?:\n(?:{'|'.join(map(re.escape, stops))})|$)", text, re.I | re.S)
        lines = []
        if raw:
            for ln in raw.group(1).splitlines():
                t = ln.strip()
                if t: lines.append(t)

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
    """
    Crawl ByName for each prefix and keep detail pages ONLY if any of:
      - effective_date
      - event_date_filed
      - event_effective_date
    is within the last `window_days` days (default: NBP_WINDOW_DAYS or 90).

    Respects:
      - MAX_DETAIL_PER_PREFIX
      - NBP_SLEEP_MS
      - NBP_STATUS_REGEX
      - NBP_TARGET_TOTAL (0 = unlimited)
    """
    today = date.today()
    if window_days is None:
        window_days = int(os.getenv("NBP_WINDOW_DAYS", "90"))
    window_start = today - timedelta(days=window_days)

    def in_window(d):
        return d is not None and window_start <= d <= today

    keep: List[Dict] = []
    CAP_TOTAL = int(os.getenv("NBP_TARGET_TOTAL", "0"))  # 0 = no cap
    NON_WINDOW_STREAK_LIMIT = int(os.getenv("NBP_NON_WINDOW_STREAK", "3"))

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

            # 1) Search by name/prefix
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

            html = page.content()
            _save_debug(f"results_{pref}", html)
            rows = _parse_results_table(html)
            if not rows:
                continue

            # Only "Active"-looking rows
            rows = [r for r in rows if _status_ok(r.get("status"))]
            if not rows:
                continue

            non_window_streak = 0

            # 2) Visit detail pages
            for row in rows[:MAX_DETAIL_PER_PREFIX]:
                if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                    break

                detail_url = row["href"]
                if not detail_url.startswith("http"):
                    detail_url = page.url.split("/Inquiry/")[0] + detail_url

                page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                _sleep(300)

                dhtml = page.content()
                if DEBUG:
                    _save_debug(f"detail_{row['doc']}", dhtml)

                try:
                    info = _parse_detail(dhtml)
                except Exception:
                    _save_debug(f"detail_parse_err_{row['doc']}", dhtml)
                    continue

                # if detail has a status and it's not Active, skip
                if info.get("status") and not _status_ok(info["status"]):
                    continue

                # ✅ Keep only if any target date is inside the window
                if any([
                    in_window(info.get("effective_date")),
                    in_window(info.get("event_date_filed")),
                    in_window(info.get("event_effective_date")),
                ]):
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
                    non_window_streak = 0
                else:
                    non_window_streak += 1
                    if non_window_streak >= NON_WINDOW_STREAK_LIMIT:
                        # We've likely paged past the recent window for this prefix
                        break

        browser.close()
    return keep


def fetch_new_by_name_prefixes(target_dates, prefixes: Iterable[str]) -> List[Dict]:
    """
    For each prefix, run a name search, open top results, and keep detail pages
    whose 'Filed On' date is in target_dates. target_dates can be a date or a set of dates.
    Respects:
      - MAX_DETAIL_PER_PREFIX (env; module-level)
      - NBP_SLEEP_MS (env; module-level)
      - NBP_STATUS_REGEX (env; module-level)
      - NBP_TARGET_TOTAL (env; total cap across all prefixes; "0" = no cap)
    """
    # Normalize input
    if isinstance(target_dates, date):
        target_dates = {target_dates}

    keep: List[Dict] = []
    CAP_TOTAL = int(os.getenv("NBP_TARGET_TOTAL", "5"))  # set to "0" for no cap
    NON_TODAY_STREAK_LIMIT = int(os.getenv("NBP_NON_TODAY_STREAK", "3"))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(os.getenv("NBP_HEADLESS", "1") == "1"))
        ctx = browser.new_context(user_agent=USER_AGENT, java_script_enabled=True, locale="en-US")
        page = ctx.new_page()

        # Warm-up
        page.goto(HOME, wait_until="networkidle", timeout=60000)
        _sleep()

        for pref in prefixes:
            # stop before processing this prefix if we've already hit the cap
            if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                break

            # 1) open ByName, fill search
            page.goto(BYNAME, wait_until="domcontentloaded", timeout=60000)
            _sleep()
            box = page.query_selector('input[name*="SearchTerm" i], input[type="text"]')
            if not box:
                _save_debug(f"byname_form_missing_{pref}", page.content())
                continue
            box.fill(pref)
            _sleep(200)

            # 2) submit
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

            html = page.content()
            _save_debug(f"results_{pref}", html)
            rows = _parse_results_table(html)
            if not rows:
                continue

            # 2.5) Only keep rows whose status looks Active
            rows = [r for r in rows if _status_ok(r.get("status"))]
            if not rows:
                continue

            # 3) open details (with early stop on non-matching streak)
            non_today_streak = 0
            for row in rows[:MAX_DETAIL_PER_PREFIX]:
                # stop mid-prefix if we've hit the cap
                if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                    break

                # Build absolute detail URL
                detail_url = row["href"]
                if not detail_url.startswith("http"):
                    detail_url = page.url.split("/Inquiry/")[0] + detail_url
                page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                _sleep(300)

                dhtml = page.content()
                if DEBUG:
                    _save_debug(f"detail_{row['doc']}", dhtml)

                # Parse detail defensively; skip bad pages
                try:
                    info = _parse_detail(dhtml)
                except Exception as e:
                    _save_debug(f"detail_parse_err_{row['doc']}", dhtml)
                    continue

                # defensive: if detail page exposes status and it's not Active, skip
                if info.get("status") and not _status_ok(info["status"]):
                    continue

                # ✅ Accept any filing date in our set
                if info["filing_date"] in target_dates:
                    keep.append({
                        "name": row["name"][:255],
                        "doc_number": row["doc"][:100],
                        "entity_type": (info.get("entity_type") or None),
                        "filing_date": info["filing_date"],
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
                    non_today_streak = 0  # reset streak on a hit

                    # stop immediately once we reach the cap
                    if CAP_TOTAL and len(keep) >= CAP_TOTAL:
                        break
                else:
                    non_today_streak += 1
                    if non_today_streak >= NON_TODAY_STREAK_LIMIT:
                        # likely past our target date range for this prefix
                        break

        browser.close()
    return keep
