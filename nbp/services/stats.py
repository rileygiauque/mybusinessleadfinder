from datetime import date
from sqlalchemy import func
from ..models import db, Jurisdiction, Entity, Stat

def _count_for_filter(jur: Jurisdiction, d0, d1):
    q = db.session.query(func.count(Entity.id))
    if jur.kind == "state":
        q = q.filter(Entity.state == "FL")
    elif jur.kind == "county":
        q = q.filter(Entity.state == "FL", Entity.county == jur.name)
    elif jur.kind == "city":
        q = q.filter(Entity.state == "FL", Entity.city == jur.name)
    if d0 and d1:
        q = q.filter(Entity.filing_date >= d0, Entity.filing_date <= d1)
    return q.scalar() or 0

def compute_stats_for_jurisdiction(jur: Jurisdiction):
    today = date.today()
    # Today
    c_today = _count_for_filter(jur, today, today)
    # MTD
    month_start = today.replace(day=1)
    c_mtd = _count_for_filter(jur, month_start, today)

    stat = Stat.query.filter_by(jurisdiction_id=jur.id, day=today).first()
    if not stat:
        stat = Stat(jurisdiction_id=jur.id, day=today)
        db.session.add(stat)
    stat.count_day = c_today
    stat.count_mtd = c_mtd
    return stat

def recompute_all_florida():
    fl = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not fl:
        return 0
    changed = 0
    for jur in [fl] + fl.children + [c for county in fl.children for c in county.children]:
        compute_stats_for_jurisdiction(jur)
        changed += 1
    db.session.commit()
    return changed
