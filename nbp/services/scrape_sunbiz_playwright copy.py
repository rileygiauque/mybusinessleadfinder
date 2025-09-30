import os, re, time
import re
from datetime import date
from typing import List, Dict, Iterable
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt

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

def _parse_detail(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Filed On / Effective Date
    filed_on = None
    m = re.search(r"(Filed On|Effective Date)\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})", text, flags=re.I)
    if m:
        try:
            filed_on = parse_dt(m.group(2)).date()
        except Exception:
            pass

    # Entity Type (best-effort)
    entity_type = None
    m2 = re.search(r"(Entity Type|Florida Profit Corporation|Limited Liability Company|Corporation)\s*:?\s*([A-Za-z ].{0,40})", text, flags=re.I)
    if m2:
        entity_type = m2.group(2).strip()[:50]

    # Registered Agent
    registered_agent = None
    m3 = re.search(r"(Registered Agent|RA Name)\s*:?\s*([A-Z0-9 ,.'\-&]+)", text, flags=re.I)
    if m3:
        registered_agent = m3.group(2).strip()[:255]

    # City, FL (from any address block)
    city = None
    m4 = re.search(r"\b([A-Z][A-Za-z .'-]+),\s*FL\b", text, flags=re.I)
    if m4:
        city = m4.group(1).strip().title()

    # Status (best-effort from detail page)
    status = None
    m5 = re.search(r"\bStatus\b\s*:?\s*([A-Za-z /-]+)", text, flags=re.I)
    if m5:
        status = m5.group(1).strip()

    return {
        "filing_date": filed_on or date.today(),
        "entity_type": entity_type,
        "registered_agent": registered_agent,
        "city": city,
        "county": None,            # can map via a city→county lookup later
        "status": status,          # may be None if not present
    }


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
                        "city": info.get("city"),
                        "county": None,
                        "registered_agent": (info.get("registered_agent") or None),
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
