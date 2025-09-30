from flask import Response, url_for
from datetime import date
from ..models import Jurisdiction, Stat

def sitemap_xml():
    fl = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not fl:
        return Response("<urlset/>", mimetype="application/xml")

    today = date.today()
    urls = []

    def add_url(loc: str, jur_id: int):
        # Look up most recent Stat for this jurisdiction
        stat = Stat.query.filter_by(jurisdiction_id=jur_id).order_by(Stat.day.desc()).first()
        lastmod = stat.day.isoformat() if stat else today.isoformat()
        urls.append((loc, lastmod))

    # Statewide
    add_url(url_for("public.state_page", _external=True), fl.id)

    # Counties + cities
    for county in fl.children:
        add_url(url_for("public.county_page", county_slug=county.slug, _external=True), county.id)

        for city in county.children:
            # Canonical city-only
            add_url(url_for("public.city_page", city_slug=city.slug, _external=True), city.id)

            # Secondary county+city (canonicalized inside page)
            add_url(url_for("public.county_city_page",
                            county_slug=county.slug,
                            city_slug=city.slug,
                            _external=True), city.id)

    # Build XML
    xml = [
        "<?xml version='1.0' encoding='UTF-8'?>",
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>",
    ]
    for loc, lastmod in urls:
        xml.append(f"  <url><loc>{loc}</loc><lastmod>{lastmod}</lastmod></url>")
    xml.append("</urlset>")

    return Response("\n".join(xml), mimetype="application/xml")
