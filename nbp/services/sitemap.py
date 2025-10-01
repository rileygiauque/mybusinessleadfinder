from flask import Response, url_for
from datetime import date
from ..models import Jurisdiction, Stat

def sitemap_xml():
    fl = Jurisdiction.query.filter_by(kind="state", slug="florida").first()
    if not fl:
        return Response(
            "<?xml version='1.0' encoding='UTF-8'?>\n<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'></urlset>",
            mimetype="application/xml"
        )

    today = date.today()
    urls = []

    def get_changefreq(population: int) -> str:
        """
        Determine change frequency based on population.
        Higher population = more business filings = more frequent updates
        """
        if population is None or population == 0:
            return "monthly"  # Default for unknown/zero population
        elif population >= 300000:
            return "daily"  # Updates every weekday
        elif population >= 100000:
            return "weekly"  # Every 2 business days ≈ weekly
        elif population >= 50000:
            return "weekly"  # Every 3 business days ≈ weekly
        elif population >= 25000:
            return "weekly"  # Every week
        else:  # 0-25k
            return "monthly"  # Every 3 weeks ≈ monthly

    def get_priority(kind: str, population: int = 0) -> str:
        """
        Calculate priority based on jurisdiction type and population.
        """
        if kind == "state":
            return "1.0"  # Highest - updated daily on weekdays
        elif kind == "county":
            # Higher population = higher priority
            if population >= 300000:
                return "0.9"
            elif population >= 100000:
                return "0.8"
            elif population >= 50000:
                return "0.7"
            else:
                return "0.6"
        elif kind == "city":
            # Cities are lower priority than counties
            if population >= 300000:
                return "0.7"
            elif population >= 100000:
                return "0.6"
            elif population >= 50000:
                return "0.5"
            else:
                return "0.4"
        else:
            return "0.5"

    def add_url(loc: str, jur: Jurisdiction):
        """Add URL with metadata based on jurisdiction data"""
        # Look up most recent filing date from Stats
        stat = Stat.query.filter_by(jurisdiction_id=jur.id).order_by(Stat.day.desc()).first()
        lastmod = stat.day.isoformat() if stat else today.isoformat()
        
        # Get population (assuming you have this field on Jurisdiction model)
        # If not, you may need to add it or estimate based on historical filing volume
        population = getattr(jur, 'population', 0)
        
        urls.append({
            'loc': loc,
            'lastmod': lastmod,
            'priority': get_priority(jur.kind, population),
            'changefreq': get_changefreq(population)
        })

    # ❌ Homepage excluded - fictitious data, rarely updated
    # (If you want it for brand/navigation purposes, add with priority 0.3, changefreq monthly)
    
    # ✅ State page - highest priority (updated every weekday)
    urls.append({
        'loc': url_for("public.state_page", _external=True),
        'lastmod': today.isoformat(),
        'priority': "1.0",
        'changefreq': "daily"  # Weekday updates
    })

    # ✅ County pages - priority/frequency based on population
    for county in fl.children:
        add_url(
            url_for("public.county_page", county_slug=county.slug, _external=True),
            county
        )

        # ✅ City pages - only canonical URLs
        for city in county.children:
            add_url(
                url_for("public.city_page", city_slug=city.slug, _external=True),
                city
            )

    # Sort by priority (descending) for cleaner sitemap
    urls.sort(key=lambda x: float(x['priority']), reverse=True)

    # ✅ Build XML
    xml = [
        "<?xml version='1.0' encoding='UTF-8'?>",
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>",
    ]
    
    for url_data in urls:
        xml.append("  <url>")
        xml.append(f"    <loc>{url_data['loc']}</loc>")
        xml.append(f"    <lastmod>{url_data['lastmod']}</lastmod>")
        xml.append(f"    <changefreq>{url_data['changefreq']}</changefreq>")
        xml.append(f"    <priority>{url_data['priority']}</priority>")
        xml.append("  </url>")
    
    xml.append("</urlset>")

    return Response("\n".join(xml), mimetype="application/xml")
