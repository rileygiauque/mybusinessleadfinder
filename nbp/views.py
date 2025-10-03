from flask import Blueprint, render_template, abort, url_for, Response, session, request, redirect, current_app
from datetime import date, timedelta
from io import StringIO
import csv
from sqlalchemy import func, or_
from flask import url_for, request
from flask import Blueprint, render_template, abort, url_for, Response, session, request, redirect
import json
from .models import Jurisdiction, Entity, Stat, Subscription, User, db

from .models import Jurisdiction, Entity, Stat

bp = Blueprint("public", __name__)

def _get_user_profile_data():
    """Get current user's profile data for display"""
    if not session.get('is_subscriber'):
        current_app.logger.warning("No is_subscriber in session")
        return None
        
    if not session.get('user_email'):
        current_app.logger.warning("No user_email in session")
        return None
    
    from .models import User, Plan
    
    user_email = session['user_email']
    current_app.logger.info(f"Looking up user: {user_email}")
    
    user = User.query.filter_by(email=user_email).first()
    if not user:
        current_app.logger.error(f"User not found: {user_email}")
        return None
    
    current_app.logger.info(f"User found: {user.email}, plan_id: {user.plan_id}")
    
    plan = Plan.query.get(user.plan_id) if user.plan_id else None
    if not plan:
        current_app.logger.error(f"Plan not found for user {user.email}")
        return None
    
    current_app.logger.info(f"Plan found: {plan.name}, price: {plan.price}")
    
    # Try to get subscription details
    subscription = Subscription.query.filter_by(
        email=user.email
    ).order_by(Subscription.created_at.desc()).first()
    
    if subscription:
        current_app.logger.info(f"Subscription found: {subscription.plan}, status: {subscription.status}")
    else:
        current_app.logger.warning(f"No subscription found for {user.email}")
    
    # Parse counties from subscription if it exists
    counties = []
    if subscription and subscription.scope_json:
        try:
            scope = json.loads(subscription.scope_json)
            counties_csv = scope.get('counties', '')
            if counties_csv:
                counties = [c.strip() for c in counties_csv.split(',') if c.strip()]
                current_app.logger.info(f"Parsed {len(counties)} counties from subscription")
        except Exception as e:
            current_app.logger.error(f"Error parsing scope_json: {e}")
    
    # Fallback to session if no counties in DB yet
    if not counties and session.get('selected_counties'):
        counties = session.get('selected_counties', [])
        current_app.logger.info(f"Using {len(counties)} counties from session")
    
    profile = {
        'email': user.email,
        'plan_name': plan.name,
        'plan_price': float(plan.price) if plan.price else 0,
        'subscription_status': user.subscription_status,
        'counties': counties,
        'county_count': len(counties),
        'trial_end_date': user.trial_end_date,
    }
    
    current_app.logger.info(f"Returning profile: {profile}")
    return profile

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

    preview_limit = int(os.getenv("NBP_PREVIEW_ROWS", "150")) if limit is None else limit

    today         = date.today()
    window_days   = int(os.getenv("NBP_WINDOW_DAYS", "90"))
    year_start    = date(today.year, 1, 1)
    ninety_start  = today - timedelta(days=window_days)
    window_start  = min(year_start, ninety_start)

    q = Entity.query.filter(
        or_(Entity.filing_date >= window_start,
            Entity.event_date_filed >= window_start)
    )

    # ✅ If ?preview=1 is in URL, always show blurred (non-subscriber view)
    if request.args.get('preview') == '1':
        if jur.kind == "state":
            q = q.filter_by(state="FL")
        elif jur.kind == "county":
            county_search = f"%{jur.name} County%"
            q = q.filter(Entity.state == "FL", Entity.county.ilike(county_search))
        elif jur.kind == "city":
            current_app.logger.info(f"Looking for city: '{jur.name}'")
            
            sample_cities = db.session.query(Entity.city).filter(
                Entity.state == "FL",
                Entity.city.isnot(None)
            ).distinct().limit(20).all()
            current_app.logger.info(f"Sample cities in DB: {[c[0] for c in sample_cities]}")
            
            q = q.filter(
                Entity.state == "FL",
                func.lower(Entity.city) == func.lower(jur.name)
            )
    
    # ✅ ENHANCED: Check for logged-in users including Local Star Plan
    elif session.get('is_subscriber') and session.get('user_email'):
        user_email = session['user_email']
        user = User.query.filter_by(email=user_email).first()
        subscription = Subscription.query.filter_by(email=user_email).first()
        
        # ✅ LOCAL STAR PLAN: Show only 15 cards from day before registration
        if user and user.plan and user.plan.name == 'Local Star':
            if user.created_at:
                registration_date = user.created_at.date()
                target_date = registration_date - timedelta(days=1)
                
                current_app.logger.info(f"Local Star user {user_email}: showing 15 records from {target_date}")
                
                # Filter to ONLY that specific day
                q = q.filter(Entity.filing_date == target_date)
                
                # Apply jurisdiction filters
                if jur.kind == "state":
                    q = q.filter_by(state="FL")
                elif jur.kind == "county":
                    county_search = f"%{jur.name} County%"
                    q = q.filter(Entity.state == "FL", Entity.county.ilike(county_search))
                elif jur.kind == "city":
                    q = q.filter(
                        Entity.state == "FL",
                        func.lower(Entity.city) == func.lower(jur.name)
                    )
                
                # Order and limit to 15
                q = q.order_by(
                    Entity.filing_date.desc().nullslast(),
                    Entity.id.desc()
                )
                
                # Force limit to 15 for Local Star users
                return q.limit(15).all()
        
        # ✅ For OTHER SUBSCRIBERS (not Local Star), apply normal access control
        if subscription and subscription.scope_json:
            scope = json.loads(subscription.scope_json)
            
            if scope.get('kind') == 'state':
                q = q.filter_by(state="FL")
            elif scope.get('kind') == 'counties':
                allowed_counties = scope.get('slugs', [])
                allowed_county_names = []
                for slug in allowed_counties:
                    county_jur = Jurisdiction.query.filter_by(
                        kind='county',
                        slug=slug
                    ).first()
                    if county_jur:
                        allowed_county_names.append(county_jur.name)
                
                if allowed_county_names:
                    # Build ILIKE conditions for each county name
                    county_conditions = [
                        Entity.county.ilike(f"%{name} County%") 
                        for name in allowed_county_names
                    ]
                    q = q.filter(
                        Entity.state == "FL",
                        or_(*county_conditions)
                    )
                else:
                    q = q.filter(Entity.id == None)
            else:
                q = q.filter(Entity.id == None)
        else:
            q = q.filter(Entity.id == None)
    
    # ✅ Not logged in - show preview
    else:
        if jur.kind == "state":
            q = q.filter_by(state="FL")
        elif jur.kind == "county":
            county_search = f"%{jur.name} County%"
            q = q.filter(Entity.state == "FL", Entity.county.ilike(county_search))
        elif jur.kind == "city":
            current_app.logger.info(f"Looking for city: '{jur.name}'")
            
            sample_cities = db.session.query(Entity.city).filter(
                Entity.state == "FL",
                Entity.city.isnot(None)
            ).distinct().limit(20).all()
            current_app.logger.info(f"Sample cities in DB: {[c[0] for c in sample_cities]}")
            
            q = q.filter(
                Entity.state == "FL",
                func.lower(Entity.city) == func.lower(jur.name)
            )

    # Normal ordering for non-Local Star users
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
    
    # ✅ Skip access control if preview mode
    if request.args.get('preview') != '1':
        if session.get('is_subscriber') and session.get('user_email'):
            subscription = Subscription.query.filter_by(
                email=session['user_email']
            ).first()
            
            if subscription and subscription.scope_json:
                scope = json.loads(subscription.scope_json)
                
                if scope.get('kind') != 'state':
                    if scope.get('kind') == 'counties' and scope.get('slugs'):
                        first_county = scope['slugs'][0]
                        return redirect(url_for('public.county_page', county_slug=first_county))
                    return redirect(url_for('public.home'))
    
    canonical = url_for("public.state_page", _external=True)
    profile_data = _get_user_profile_data()
    
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=_children(jur),
        canonical_url=canonical,
        parent_state=None,
        parent_county=None,
        profile_data=profile_data,
    )

@bp.get("/api/nearby-cities")
def nearby_cities():
    """Return nearby cities based on ZIP code"""
    zip_code = request.args.get('zip', '').strip()
    
    if not zip_code:
        return {'cities': ['Miami', 'Fort Lauderdale', 'West Palm Beach']}, 200
    
    # Simple fallback - you could enhance this with a real ZIP lookup
    # For now, just return Florida cities
    return {'cities': ['Miami', 'Fort Lauderdale', 'West Palm Beach']}, 200

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

    # ✅ Skip access control if preview mode
    if request.args.get('preview') != '1':
        if session.get('is_subscriber') and session.get('user_email'):
            subscription = Subscription.query.filter_by(
                email=session['user_email']
            ).first()
            
            if subscription and subscription.scope_json:
                scope = json.loads(subscription.scope_json)
                
                has_access = (
                    scope.get('kind') == 'state' or
                    (scope.get('kind') == 'counties' and 
                     county_slug in scope.get('slugs', []))
                )
                
                if not has_access:
                    if scope.get('kind') == 'counties' and scope.get('slugs'):
                        return redirect(url_for('public.county_page', county_slug=scope['slugs'][0]))
                    elif scope.get('kind') == 'state':
                        return redirect(url_for('public.state_page'))
                    return redirect(url_for('public.home'))

    canonical = url_for("public.county_page", county_slug=jur.slug, _external=True)
    profile_data = _get_user_profile_data()
    
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=_children(jur),
        canonical_url=canonical,
        parent_state=state,
        parent_county=None,
        profile_data=profile_data,
    )


@bp.get("/new-business/florida/city/<city_slug>/")
def city_page(city_slug):
    state = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not state:
        abort(404)
    jur = Jurisdiction.query.filter_by(kind="city", slug=city_slug).first()
    if not jur:
        abort(404)

    # ✅ Skip access control if preview mode
    if request.args.get('preview') != '1':
        if session.get('is_subscriber') and session.get('user_email'):
            subscription = Subscription.query.filter_by(
                email=session['user_email']
            ).first()
            
            if subscription and subscription.scope_json:
                scope = json.loads(subscription.scope_json)
                
                city_county = jur.parent if jur.parent else None
                city_county_slug = city_county.slug if city_county else None
                
                has_access = (
                    scope.get('kind') == 'state' or
                    (scope.get('kind') == 'counties' and 
                     city_county_slug in scope.get('slugs', []))
                )
                
                if not has_access:
                    if scope.get('kind') == 'counties' and scope.get('slugs'):
                        return redirect(url_for('public.county_page', county_slug=scope['slugs'][0]))
                    elif scope.get('kind') == 'state':
                        return redirect(url_for('public.state_page'))
                    return redirect(url_for('public.home'))

    canonical = url_for("public.city_page", city_slug=jur.slug, _external=True)
    profile_data = _get_user_profile_data()
    
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=[],
        canonical_url=canonical,
        parent_state=state,
        parent_county=jur.parent if jur.parent else None,
        profile_data=profile_data,
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

    # ✅ Skip access control if preview mode
    if request.args.get('preview') != '1':
        if session.get('is_subscriber') and session.get('user_email'):
            subscription = Subscription.query.filter_by(
                email=session['user_email']
            ).first()
            
            if subscription and subscription.scope_json:
                scope = json.loads(subscription.scope_json)
                
                has_access = (
                    scope.get('kind') == 'state' or
                    (scope.get('kind') == 'counties' and 
                     county_slug in scope.get('slugs', []))
                )
                
                if not has_access:
                    if scope.get('kind') == 'counties' and scope.get('slugs'):
                        return redirect(url_for('public.county_page', county_slug=scope['slugs'][0]))
                    elif scope.get('kind') == 'state':
                        return redirect(url_for('public.state_page'))
                    return redirect(url_for('public.home'))

    canonical = url_for("public.city_page", city_slug=jur.slug, _external=True)
    profile_data = _get_user_profile_data()
    
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=_get_stats(jur.id),
        sample=_get_sample_rows(jur),
        children=[],
        canonical_url=canonical,
        parent_state=state,
        parent_county=county,
        profile_data=profile_data,
    )


@bp.get("/new-business/florida/multi/")
def multi_counties_page():
    raw = (request.args.get("counties") or "").strip()
    slugs = [s for s in raw.split(",") if s]

    if len(slugs) < 2:
        return redirect(url_for("public.state_page"))

    # ✅ Skip access control if preview mode
    if request.args.get('preview') != '1':
        if session.get('is_subscriber') and session.get('user_email'):
            subscription = Subscription.query.filter_by(
                email=session['user_email']
            ).first()
            
            if subscription and subscription.scope_json:
                scope = json.loads(subscription.scope_json)
                
                if scope.get('kind') == 'counties':
                    allowed = scope.get('slugs', [])
                    slugs = [s for s in slugs if s in allowed]
                    
                    if not slugs:
                        return redirect(url_for('public.county_page', county_slug=allowed[0]))

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

    class _J: pass
    jur = _J()
    jur.kind = "multi"
    head = ", ".join(county_names[:2]) if len(county_names) > 1 else county_names[0]
    tail = f" (+{len(county_names)-2})" if len(county_names) > 2 else ""
    jur.name = f"{head}{tail}"
    jur.slug = "multi"

    canonical = url_for("public.state_page", _external=True) + "?counties=" + ",".join(slugs)
    profile_data = _get_user_profile_data()
    
    return render_template(
        "nb_page.html",
        jur=jur,
        stats=None,
        sample=sample,
        children=[],
        canonical_url=canonical,
        parent_state=state,
        parent_county=None,
        profile_data=profile_data,
    )
    
@bp.get("/export/<path:slug>.csv")
def export_csv(slug):
    if not session.get("is_subscriber"):
        abort(403)

    jur = Jurisdiction.query.filter_by(slug=slug).first()
    if not jur:
        abort(404)

    # ✅ Check if user has access to export this jurisdiction
    subscription = Subscription.query.filter_by(
        email=session['user_email']
    ).first()
    
    if subscription and subscription.scope_json:
        scope = json.loads(subscription.scope_json)
        
        # Check access based on jurisdiction type
        if jur.kind == "state":
            if scope.get('kind') != 'state':
                abort(403)
        elif jur.kind == "county":
            has_access = (
                scope.get('kind') == 'state' or
                (scope.get('kind') == 'counties' and jur.slug in scope.get('slugs', []))
            )
            if not has_access:
                abort(403)
        elif jur.kind == "city":
            city_county_slug = jur.parent.slug if jur.parent else None
            has_access = (
                scope.get('kind') == 'state' or
                (scope.get('kind') == 'counties' and city_county_slug in scope.get('slugs', []))
            )
            if not has_access:
                abort(403)

    since = date.today() - timedelta(days=30)
    q = Entity.query.filter(Entity.filing_date >= since)
    
    if jur.kind == "state":
        q = q.filter_by(state="FL")
    elif jur.kind == "county":
        county_search = f"%{jur.name} County%"
        q = q.filter(Entity.state == "FL", Entity.county.ilike(county_search))
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

@bp.post("/api/hero-finish")
def hero_finish():
    """
    Handle the hero form 'Finish' button - simple lead capture
    (does NOT create account or subscription)
    """
    try:
        from datetime import datetime
        from .utils import send_telegram_notification
        
        data = request.get_json()
        email = data.get('email', '').strip()
        phone = data.get('phone', '').strip()
        state = data.get('state', 'Florida')
        counties = data.get('counties', '')
        
        if not email:
            return {'success': False, 'error': 'Email required'}, 400
        
        # Send Telegram notification for hero form
        send_telegram_notification({
            'email': email,
            'phone': phone or 'Not provided',
            'state': state,
            'counties': counties,
            'plan_name': 'Lead Capture (No Plan Yet)',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source': '🎯 Hero Form'
        })
        
        return {'success': True}
        
    except Exception as e:
        current_app.logger.error(f"Hero finish error: {e}")
        return {'success': False, 'error': str(e)}, 500
