# nbp/services/scrape_sunbiz.py
import os, time, random, re
from datetime import date, timedelta
from typing import Iterable, Dict, Optional, List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dateutil.parser import parse as parse_dt

from ..models import db, Entity

USER_AGENT = os.getenv("NBP_UA", "NewBizPulseBot/0.1 (contact: you@example.com)")
BASE_LIST = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByDate"     # form page
# We will discover the POST action from the <form>, but you can force it here if needed:
FORCED_POST = os.getenv("NBP_SUNBIZ_POST", "").strip()  # e.g. "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchByDate"

DEBUG = os.getenv("NBP_DEBUG", "0") == "1"

def _sleep():
    time.sleep(random.uniform(0.6, 1.6))

def polite_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def _collect_form_payload(form: BeautifulSoup) -> Dict[str, str]:
    """
    Collect all <input> fields from the form, including hidden ASP.NET fields:
    __RequestVerificationToken, __VIEWSTATE, __EVENTVALIDATION, etc.
    """
    payload = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        val = inp.get("value", "")
        payload[name] = val
    # Some sites use <select>/<textarea> – add if needed
    return payload

def _first_form_and_action(soup: BeautifulSoup, current_url: str) -> (Optional[BeautifulSoup], Optional[str]):
    form = soup.find("form")
    if not form:
        return None, None
    action = form.get("action") or ""
    action_abs = urljoin(current_url, action)
    return form, action_abs

def _save_debug_html(name: str, html: str):
    if not DEBUG:
        return
    path = f"debug_{name}.html"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[sunbiz] wrote debug HTML → {path}")
    except Exception as e:
        print("[sunbiz] failed to write debug:", e)

def _find_results_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    Heuristic: pick the table whose header contains common labels like
    'Document Number' or 'Entity Name' or 'Filing Date'.
    """
    candidates = soup.find_all("table")
    best = None
    best_score = 0
    for t in candidates:
        text = t.get_text(" ", strip=True).lower()
        score = 0
        for needle in ("document", "doc", "entity", "name", "filing", "date"):
            if needle in text:
                score += 1
        if score > best_score and t.find("tr"):
            best = t
            best_score = score
    return best

def parse_filing_row(tr) -> Optional[Dict]:
    tds = tr.find_all("td")
    if len(tds) < 2:
        return None

    # Try to locate a doc number (often a separate column or within a link)
    # We’ll extract text from each cell and guess columns.
    cells = [td.get_text(" ", strip=True) for td in tds]

    # Name: prefer the first anchor text if present, else first cell
    name = ""
    first_a = tds[0].find("a")
    if first_a and first_a.get_text(strip=True):
        name = first_a.get_text(strip=True)
    else:
        name = cells[0] if cells else ""

    # Doc number: search any cell for an alphanumeric doc-like token
    doc_number = ""
    for c in cells[:3]:  # usually early columns
        m = re.search(r"[A-Z0-9]{6,}", c.replace(" ", ""))
        if m:
            doc_number = m.group(0)
            break

    # Entity type: best-effort guess from one of the cells
    entity_type = ""
    for c in cells:
        if any(k in c.upper() for k in ("LLC", "CORP", "INC", "L.L.C", "CO. ", "COMPANY")):
            entity_type = c.split()[0][:50]
            break

    # Filing date: sniff mm/dd/yyyy in any cell
    filing_date = date.today()
    for c in cells:
        if re.search(r"\d{1,2}/\d{1,2}/\d{4}", c):
            try:
                filing_date = parse_dt(c, dayfirst=False).date()
                break
            except Exception:
                pass

    if not name or not doc_number:
        return None

    # Optional: follow detail link to get more fields (city/county/agent).
    # detail_a = first_a or None
    # if detail_a and detail_a.get("href"):
    #     detail_url = urljoin(BASE_LIST, detail_a["href"])
    #     det = polite_session().get(detail_url, timeout=30)
    #     _sleep()
    #     if det.ok:
    #         dsoup = BeautifulSoup(det.text, "html.parser")
    #         # TODO: parse address/agent labels to fill city/county/registered_agent

    return {
        "name": name[:255],
        "doc_number": doc_number[:100],
        "entity_type": (entity_type[:50] or None),
        "filing_date": filing_date,
        "city": None,
        "county": None,
        "registered_agent": None,
    }

def _parse_table_rows(table: BeautifulSoup) -> List[Dict]:
    filings = []
    for tr in table.find_all("tr"):
        # skip header rows
        if tr.find("th"):
            continue
        rec = parse_filing_row(tr)
        if rec:
            filings.append(rec)
    return filings

def _find_pagination_links(soup: BeautifulSoup, current_url: str) -> List[str]:
    """
    Try to find additional result pages.
    Many ASP.NET tables render paging links as anchors with query args.
    """
    links = []
    for a in soup.find_all("a"):
        txt = a.get_text(strip=True).lower()
        if txt in {"next", ">", "»"} or re.match(r"^\d+$", txt):
            href = a.get("href")
            if href:
                links.append(urljoin(current_url, href))
    # De-dup while preserving order
    out, seen = [], set()
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def fetch_by_date(d: date) -> Iterable[Dict]:
    s = polite_session()

    # 1) GET the form page to capture hidden fields + action
    r1 = s.get(BASE_LIST, timeout=30)
    r1.raise_for_status()
    _sleep()
    _save_debug_html(f"sunbiz_form_{d.isoformat()}", r1.text)

    soup1 = BeautifulSoup(r1.text, "html.parser")
    form, action_url = _first_form_and_action(soup1, r1.url)
    if not form:
        print("[sunbiz] no form on page; layout changed?")
        return []
    post_url = FORCED_POST or action_url
    payload = _collect_form_payload(form)

    # 2) Fill in date fields (common names shown; adjust if your form uses different)
    # Inspect the form markup; if the inputs are called different names, we still set them:
    date_str = d.strftime("%m/%d/%Y")
    for key in ("FromDate", "Fromdate", "fromdate", "fromDate"):
        if key in payload or True:  # ensure we set something
            payload[key] = date_str
            break
    for key in ("ToDate", "Todate", "todate", "toDate"):
        if key in payload or True:
            payload[key] = date_str
            break

    # Some forms need a submit button name/value:
    # (If you see it in DevTools, set it; otherwise harmless.)
    payload.setdefault("SearchButton", "Search")

    # 3) POST the form
    r2 = s.post(post_url, data=payload, timeout=30)
    r2.raise_for_status()
    _sleep()
    _save_debug_html(f"sunbiz_results_{d.isoformat()}_page1", r2.text)

    soup2 = BeautifulSoup(r2.text, "html.parser")
    table = _find_results_table(soup2)
    filings = _parse_table_rows(table) if table else []

    # 4) Try pagination (follow “Next”/page number links on first page only)
    if table:
        pages = _find_pagination_links(soup2, r2.url)
        # Limit pages to be polite
        for i, url in enumerate(pages[:9], start=2):
            r = s.get(url, timeout=30)
            if not r.ok:
                break
            _sleep()
            _save_debug_html(f"sunbiz_results_{d.isoformat()}_page{i}", r.text)
            soup = BeautifulSoup(r.text, "html.parser")
            t = _find_results_table(soup)
            if not t:
                break
            filings += _parse_table_rows(t)

    return filings

def upsert_entity(rec: Dict) -> bool:
    existing = Entity.query.filter_by(doc_number=rec["doc_number"]).first()
    if existing:
        changed = False
        for k in ("name", "entity_type", "filing_date", "city", "county", "registered_agent"):
            v = rec.get(k)
            if v and getattr(existing, k) != v:
                setattr(existing, k, v)
                changed = True
        if changed:
            db.session.add(existing)
        return changed
    else:
        e = Entity(
            name=rec.get("name") or "",
            entity_type=rec.get("entity_type"),
            filing_date=rec.get("filing_date") or date.today(),
            city=(rec.get("city") or "")[:255],
            county=(rec.get("county") or "")[:255],
            state="FL",
            registered_agent=(rec.get("registered_agent") or "")[:255],
            doc_number=rec.get("doc_number")[:100],
        )
        db.session.add(e)
        return True

def run_sunbiz_scrape(days_back: int = 1, max_rows: int = 2000, dry_run: bool = False) -> Dict:
    total_seen = 0
    total_upserted = 0

    today = date.today()
    start = today - timedelta(days=days_back - 1)
    all_days = [start + timedelta(days=i) for i in range(days_back)]

    for d in all_days:
        rows = list(fetch_by_date(d))
        if DEBUG and not rows:
            print("[sunbiz] 0 rows parsed; check debug HTML files for", d.isoformat())
        for rec in rows:
            total_seen += 1
            if dry_run:
                continue
            if upsert_entity(rec):
                total_upserted += 1
            if total_seen >= max_rows:
                break
        if not dry_run:
            db.session.commit()
        if total_seen >= max_rows:
            break

    return {"days": len(all_days), "seen": total_seen, "upserted": total_upserted, "dry_run": dry_run}
