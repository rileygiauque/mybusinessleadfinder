from flask import Flask, request, jsonify, redirect, url_for
from flask_migrate import Migrate
import os
from .models import db, User, Plan
from .views import bp as public_bp
from .services.sitemap import sitemap_xml
from .billing import bp as billing_bp
from .services.robots import robots_txt
from dotenv import load_dotenv
load_dotenv()

def create_user(email, password, plan_name):
    # Find the plan
    plan = Plan.query.filter_by(name=plan_name).first()

    if not plan:
        print(f"Plan '{plan_name}' not found!")
        return None

    # Create new user and hash their password
    new_user = User(email=email, plan=plan)
    new_user.set_password(password)
    
    # Add the user to the session and commit to the database
    db.session.add(new_user)
    db.session.commit()

    return new_user


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    # ⬇️ import the blueprint here and register it AFTER app exists
    from .nearby_cities_api import bp as nearby_bp
    app.register_blueprint(nearby_bp)

    # Use DATABASE_URL if provided, else fallback to local SQLite
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Render, Railway, Heroku often provide DATABASE_URL with "postgres://"
        # SQLAlchemy requires "postgresql://"
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    else:
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///local.db"

    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

    db.init_app(app)
    Migrate(app, db)

    # Public pages
    app.register_blueprint(public_bp)
    app.register_blueprint(billing_bp)

    # Sitemap
    @app.get("/sitemap.xml")
    def sitemap():
        return sitemap_xml()

    # Debug: show all routes (temporary)
    @app.get("/__routes")
    def routes():
        return {"routes": sorted([str(r) for r in app.url_map.iter_rules()])}

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/robots.txt")
    def robots():
        return robots_txt()

    # ⚠️ REMOVE OR COMMENT THIS OUT FOR PRODUCTION
    # This will try to create a user EVERY TIME the app starts
    # with app.app_context():
    #     create_user('test@example.com', 'securepassword', 'Local Star')
    #     print("User created successfully.")

    return app
