from flask import Response, url_for

def robots_txt():
    try:
        sitemap_url = url_for('public.sitemap_xml', _external=True)
    except:
        sitemap_url = "/sitemap.xml"
    
    lines = [
        "User-agent: *",
        "Allow: /new-business/",  # ✅ Real data pages
        "Disallow: /export/",
        "Disallow: /api/",
        "Disallow: /*?preview=1",
        "",
        # ✅ Optional: Deprioritize homepage since it's fictitious
        "# Homepage has demo data only - business data at /new-business/florida/",
        "",
        f"Sitemap: {sitemap_url}",
    ]
    return Response("\n".join(lines), mimetype="text/plain")
