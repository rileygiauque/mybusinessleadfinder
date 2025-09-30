from flask import Response

def robots_txt():
    lines = [
        "User-agent: *",
        "Allow: /new-business/",
        "Disallow: /export/",
        "",
        "Sitemap: /sitemap.xml",
    ]
    return Response("\n".join(lines), mimetype="text/plain")
