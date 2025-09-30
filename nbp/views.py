from flask import Blueprint, render_template, abort, url_for, Response, session, request
from datetime import date, timedelta
from io import StringIO
import csv
from sqlalchemy import func, or_
from flask import url_for, request
from flask import Blueprint, render_template, abort, url_for, Response, session, request, redirect
import json
from .models import Jurisdiction, Entity, Stat, Subscription, db


from .models import Jurisdiction, Entity, Stat

bp = Blueprint("public", __name__)


def _get_stats(jur_id):
    today = date.today()
    s = Stat.query.filter_by(jurisdiction_id=jur_id, day=today).first()

    total = (
        Stat.query.filter_by(jurisdiction_id=jur_id)
        .with_entities(func.sum(Stat.count_day))
        .scalar()
        or 0
    )

    return {
        "today": s.count_day if s else 0,
        "mtd": s.count_mtd if s else 0,
        "total": total,
        "asof": today.isoformat(),
    }


def _get_sample_rows(jur: Jurisdiction, limit=None):
    from datetime import date, timedelta
    import os

    # preview size (what the public page will show)
    preview_limit = int(os.getenv("NBP_PREVIEW_ROWS", "150")) if limit is None else limit

    # window start = earlier of (year-to-date) OR (last NBP_WINDOW_DAYS, default 90)
    today         = date.today()
    window_days   = int(os.getenv("NBP_WINDOW_DAYS", "90"))
    year_start    = date(today.year, 1, 1)
    ninety_start  = today - timedelta(days=window_days)
    window_start  = min(year_start, ninety_start)

    # base query: within window by filing_date OR event_date_filed
    q = Entity.query.filter(
        or_(Entity.filing_date >= window_start,
            Entity.event_date_filed >= window_start)
    )

    # scope to jurisdiction
    if jur.kind == "state":
        q = q.filter_by(state="FL")
    elif jur.kind == "county":
        q = q.filter_by(state="FL", county=jur.name)
    elif jur.kind == "city":
        q = q.filter_by(state="FL", city=jur.name)

    # newest first
    q = q.order_by(
        Entity.filing_date.desc().nullslast(),
        Entity.event_date_filed.desc().nullslast(),
        Entity.id.desc(),
    )

    return q.limit(preview_limit).all()


def _children(jur: Jurisdiction):
    return sorted(jur.children, key=lambda c: c.name)[:12]


@bp.get("/")
def home():
    from .models import Entity, Jurisdiction

    # Homepage preview records
    sample = Entity.query.order_by(Entity.filing_date.desc()).limit(20).all()

    # Pull all Florida counties (children of state "florida")
    fl = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    counties = []
    if fl:
        counties = (
            Jurisdiction.query
            .filter_by(kind="county", parent_id=fl.id)
            .order_by(Jurisdiction.name.asc())
            .all()
        )

    return render_template(
        "index.html",
        sample=sample,
        Jurisdiction=Jurisdiction,
        counties=counties,           # <-- important
    )




@bp.get("/new-business/florida/")
def state_page():
    jur = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not jur:
        abort(404)
    canonical = url_for("public.state_page", _external=True)
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=_children(jur),
        canonical_url=canonical,
        parent_state=None,
        parent_county=None,
    )


@bp.get("/new-business/florida/county/<county_slug>/")
def county_page(county_slug):
    state = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not state:
        abort(404)
    jur = Jurisdiction.query.filter_by(
        kind="county", slug=county_slug, parent_id=state.id
    ).first()
    if not jur:
        abort(404)

    canonical = url_for("public.county_page", county_slug=jur.slug, _external=True)

    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=_children(jur),
        canonical_url=canonical,
        parent_state=state,
        parent_county=None,
    )


@bp.get("/new-business/florida/city/<city_slug>/")
def city_page(city_slug):
    state = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not state:
        abort(404)
    jur = Jurisdiction.query.filter_by(kind="city", slug=city_slug).first()
    if not jur:
        abort(404)

    canonical = url_for("public.city_page", city_slug=jur.slug, _external=True)

    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=[],
        canonical_url=canonical,
        parent_state=state,
        parent_county=jur.parent if jur.parent else None,
    )


@bp.get("/new-business/florida/county/<county_slug>/city/<city_slug>/")
def county_city_page(county_slug, city_slug):
    state = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not state:
        abort(404)
    county = Jurisdiction.query.filter_by(
        kind="county", slug=county_slug, parent_id=state.id
    ).first()
    if not county:
        abort(404)
    jur = Jurisdiction.query.filter_by(
        kind="city", slug=city_slug, parent_id=county.id
    ).first()
    if not jur:
        abort(404)

    canonical = url_for("public.city_page", city_slug=jur.slug, _external=True)

    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=[],
        canonical_url=canonical,
        parent_state=state,
        parent_county=county,
    )

@bp.get("/new-business/florida/multi/")
def multi_counties_page():
    # Expect: /new-business/florida/multi/?counties=miami-dade,broward,...
    raw = (request.args.get("counties") or "").strip()
    slugs = [s for s in raw.split(",") if s]

    # Need at least 2 to be a "multi" view; otherwise send them to statewide
    if len(slugs) < 2:
        return redirect(url_for("public.state_page"))

    # Validate slugs against Florida counties
    state = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not state:
        abort(404)

    counties = (Jurisdiction.query
                .filter(Jurisdiction.kind == "county",
                        Jurisdiction.parent_id == state.id,
                        Jurisdiction.slug.in_(slugs))
                .order_by(Jurisdiction.name.asc())
                .all())

    if not counties:
        abort(404)

    county_names = [c.name for c in counties]

    # Build a recent window identical to _get_sample_rows so the UX matches
    from datetime import date, timedelta
    import os
    today = date.today()
    window_days  = int(os.getenv("NBP_WINDOW_DAYS", "90"))
    year_start   = date(today.year, 1, 1)
    ninety_start = today - timedelta(days=window_days)
    window_start = min(year_start, ninety_start)

    q = (Entity.query
         .filter(
             or_(Entity.filing_date >= window_start,
                 Entity.event_date_filed >= window_start),
             Entity.state == "FL",
             Entity.county.in_(county_names)
         )
         .order_by(
             Entity.filing_date.desc().nullslast(),
             Entity.event_date_filed.desc().nullslast(),
             Entity.id.desc(),
         ))

    preview_limit = int(os.getenv("NBP_PREVIEW_ROWS", "15"))
    sample = q.limit(preview_limit).all()

    # Minimal jur object so nb_page.html titles/meta work nicely
    class _J: pass
    jur = _J()
    jur.kind = "multi"
    # e.g., "Miami-Dade, Broward (+8)"
    head = ", ".join(county_names[:2]) if len(county_names) > 1 else county_names[0]
    tail = f" (+{len(county_names)-2})" if len(county_names) > 2 else ""
    jur.name = f"{head}{tail}"
    jur.slug = "multi"

    canonical = url_for("public.state_page", _external=True) + "?counties=" + ",".join(slugs)

    return render_template(
        "nb_page.html",
        jur=jur,
        stats=None,                 # optional; template doesn’t need stats for multi
        sample=sample,
        children=[],                # not showing children for multi
        canonical_url=canonical,
        parent_state=state,
        parent_county=None,
    )

@bp.get("/subscribe")
def subscribe_get():
    # Optional landing page if someone hits /subscribe directly
    return render_template("subscribe.html")

@bp.post("/subscribe")
def subscribe_post():
    email = (request.form.get("email") or "").strip().lower()

    # Accept CSV from the wizard (e.g., "florida" or "miami-dade,broward")
    counties_csv = (request.form.get("counties") or "").strip()
    selected = [s.strip() for s in counties_csv.split(",") if s.strip()]
    is_statewide = "florida" in selected


    # Build scope to store
    if is_statewide:
        scope = {"kind": "state", "slug": "florida"}
    else:
        scope = {"kind": "counties", "slugs": [s for s in selected if s]}

    # (Optional) validate county slugs belong to Florida
    fl = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if fl and scope.get("kind") == "counties":
        valid_slugs = {
            s for (s,) in Jurisdiction.query
                     .filter(Jurisdiction.kind == "county", Jurisdiction.parent_id == fl.id)
                     .with_entities(Jurisdiction.slug)
                     .all()
        }
        scope["slugs"] = [s for s in scope["slugs"] if s in valid_slugs]

    # Enforce one-time free access per email
    existing = Subscription.query.filter_by(email=email).first()
    if existing and existing.status in ("free_claimed",):
        # Not a paying user; send to the indexed page they chose (shows paywall there)
        redir = request.form.get("redirect_to")
        if not redir:
            if is_statewide:
                redir = url_for("public.state_page")
            elif scope.get("kind") == "counties" and scope.get("slugs"):
                redir = url_for("public.county_page", county_slug=scope["slugs"][0])
            else:
                redir = url_for("public.state_page")
        return redirect(redir)

    if existing and existing.status == "active":
        # Mark them as subscriber for this session and send to their page
        session["is_subscriber"] = True
        redir = request.form.get("redirect_to") or url_for("public.state_page")
        return redirect(redir)


    # Create or update a record and mark free trial as claimed
    sub = existing or Subscription(email=email, plan="lead")
    if sub.id is None:
        db.session.add(sub)
    sub.plan = sub.plan or "lead"
    sub.status = "free_claimed"
    sub.scope_json = json.dumps(scope, ensure_ascii=False)
    db.session.commit()

    # Redirect to the indexed page for their selection
    redir = request.form.get("redirect_to")
    if not redir:
        # Fallback server-side computation if client didn't send redirect
        if is_statewide:
            redir = url_for("public.state_page")
        elif scope.get("kind") == "counties" and scope.get("slugs"):
            redir = url_for("public.county_page", county_slug=scope["slugs"][0])
        else:
            redir = url_for("public.state_page")

    return redirect(redir)




@bp.get("/export/<path:slug>.csv")
def export_csv(slug):
    if not session.get("is_subscriber"):
        abort(403)

    jur = Jurisdiction.query.filter_by(slug=slug).first()
    if not jur:
        abort(404)

    since = date.today() - timedelta(days=30)
    q = Entity.query.filter(Entity.filing_date >= since)
    if jur.kind == "state":
        q = q.filter_by(state="FL")
    elif jur.kind == "county":
        q = q.filter_by(state="FL", county=jur.name)
    elif jur.kind == "city":
        q = q.filter_by(state="FL", city=jur.name)

    rows = q.order_by(Entity.filing_date.desc()).all()

    out = StringIO()
    w = csv.writer(out)
    w.writerow(
        ["name", "entity_type", "filing_date", "city", "county", "state", "registered_agent", "doc_number"]
    )
    for e in rows:
        w.writerow([
            e.name,
            e.entity_type or "",
            e.filing_date.isoformat() if e.filing_date else "",
            e.city or "",
            e.county or "",
            e.state or "",
            e.registered_agent or "",
            e.doc_number or "",
        ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={slug}.csv"},
    )
