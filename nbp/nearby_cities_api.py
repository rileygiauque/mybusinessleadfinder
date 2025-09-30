# nearby_cities_api.py  (or paste into app.py)
from flask import Blueprint, jsonify, request, abort
import pandas as pd
import os, bisect

bp = Blueprint("nearby", __name__)

EXCEL_PATH = os.path.join(os.path.dirname(__file__), "yourfile.xlsx")

def _load_zip_table(path=EXCEL_PATH):
    # Keep leading zeros in ZIP/CC
    df = pd.read_excel(path, engine="openpyxl", dtype={"Zip": str, "CC": str, "City": str, "State": str, "County": str})
    # Clean up
    for col in ("Zip","CC","City","State","County"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    df["Zip"] = df["Zip"].str.extract(r"(\d{3,5})", expand=False).fillna("").str.zfill(5)
    df = df[df["Zip"] != ""]  # keep valid zips

    # Main structures
    # 1) zip -> city list (unique, title-case)
    zip_to_cities = {}
    # 2) zip -> county code (CC) and state
    zip_to_cc = {}
    zip_to_state = {}

    # 3) county code -> {zip set, city set}
    cc_to_zips = {}
    cc_to_cities = {}

    for _, r in df.iterrows():
        z  = r["Zip"]
        ct = (r.get("City") or "").title()
        st = (r.get("State") or "").upper()
        cc = (r.get("CC") or "").zfill(5)  # CCs look like 12019, etc.

        if z not in zip_to_cities: zip_to_cities[z] = []
        if ct and ct not in zip_to_cities[z]:
            zip_to_cities[z].append(ct)

        zip_to_cc[z] = cc
        zip_to_state[z] = st

        cc_to_zips.setdefault(cc, set()).add(z)
        if ct: cc_to_cities.setdefault(cc, set()).add(ct)

    # Sorted zips per county for numeric-nearest fallback
    cc_to_sorted_zips = {cc: sorted(int(z) for z in zs if z.isdigit()) for cc, zs in cc_to_zips.items()}
    # Statewide sorted for final fallback
    statewide_sorted = sorted(int(z) for z in zip_to_cities.keys() if z.isdigit())

    return zip_to_cities, zip_to_cc, zip_to_state, cc_to_cities, cc_to_sorted_zips, statewide_sorted

ZIP2CITY, ZIP2CC, ZIP2STATE, CC2CITIES, CC2SORTEDZIPS, STATEWIDE_SORTED = _load_zip_table()

def _nearest_in_sorted(target_zip_str, sorted_list_ints, take=6):
    try:
        t = int(target_zip_str)
    except:
        return []
    i = bisect.bisect_left(sorted_list_ints, t)
    out = []
    l, r = i-1, i
    while (l>=0 or r<len(sorted_list_ints)) and len(out)<take:
        ld = abs(sorted_list_ints[l]-t) if l>=0 else None
        rd = abs(sorted_list_ints[r]-t) if r<len(sorted_list_ints) else None
        if ld is None:
            out.append(sorted_list_ints[r]); r+=1
        elif rd is None:
            out.append(sorted_list_ints[l]); l-=1
        elif ld <= rd:
            out.append(sorted_list_ints[l]); l-=1
        else:
            out.append(sorted_list_ints[r]); r+=1
    return [str(z).zfill(5) for z in out]

@bp.route("/api/nearby-cities")
def nearby_cities():
    zipq = (request.args.get("zip") or request.args.get("zipcode") or "").strip()
    if not zipq:
        return abort(400, "usage: /api/nearby-cities?zip=32003")
    zipq = zipq[:5].zfill(5)

    # 1) exact zip city list
    result = []
    exact = ZIP2CITY.get(zipq, [])
    for c in exact:
        if c not in result: result.append(c)
        if len(result) >= 3: break
    if len(result) >= 3:
        return jsonify({"zip": zipq, "cities": result, "source": "zip-exact"})

    # 2) same-county expansions
    cc = ZIP2CC.get(zipq, "")
    if cc:
        # other cities in same county
        for c in CC2CITIES.get(cc, []):
            if c not in result: result.append(c)
            if len(result) >= 3: break
        if len(result) < 3:
            # numeric-nearest zips within same county, then take their primary city
            for nz in _nearest_in_sorted(zipq, CC2SORTEDZIPS.get(cc, []), take=10):
                if nz == zipq: continue
                for c in ZIP2CITY.get(nz, []):
                    if c not in result: result.append(c)
                    break
                if len(result) >= 3: break
        if len(result) >= 3:
            return jsonify({"zip": zipq, "cities": result[:3], "source": "county-nearest"})

    # 3) statewide numeric-nearest fallback
    for nz in _nearest_in_sorted(zipq, STATEWIDE_SORTED, take=10):
        for c in ZIP2CITY.get(nz, []):
            if c not in result: result.append(c); break
        if len(result) >= 3: break

    if not result:
        result = ["Miami", "Fort Lauderdale", "West Palm Beach"]  # ultimate fallback

    return jsonify({"zip": zipq, "cities": result[:3], "source": "statewide"})
