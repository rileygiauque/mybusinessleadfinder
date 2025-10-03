# nbp/models.py
from datetime import datetime, date
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Index, UniqueConstraint
import json
from werkzeug.security import generate_password_hash, check_password_hash


db = SQLAlchemy()  # define once, here — no self-imports

# Plan Model
class Plan(db.Model):
    __tablename__ = 'plans'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    price = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(255))

    # A plan can have many users
    users = db.relationship('User', backref='plan', lazy=True)

# User Model
class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    plan_id = db.Column(db.Integer, db.ForeignKey('plans.id'), nullable=False)  # ForeignKey to the Plan
    subscription_status = db.Column(db.String(50), default="inactive")  # active, trial, etc.
    trial_end_date = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)  # ✅ Add this if missing
    
    # Method to hash password when setting it
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    # Method to check if the entered password matches the stored hash
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    # Method to assign a plan to the user
    def assign_plan(self, plan):
        self.plan = plan

    def __repr__(self):
        return f'<User {self.email}>'


class Jurisdiction(db.Model):
    __tablename__ = "jurisdictions"
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(50), nullable=False)  # state | county | city
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False, unique=True)
    parent_id = db.Column(db.Integer, db.ForeignKey("jurisdictions.id"))
    parent = db.relationship("Jurisdiction", remote_side=[id], backref="children")
    
    population = db.Column(db.Integer, nullable=True, default=0)

class Entity(db.Model):
    __tablename__ = "entities"
    id = db.Column(db.Integer, primary_key=True)
    name        = db.Column("entity_name", db.String(255), nullable=False, index=True)

    entity_type = db.Column("entity_type", db.String(50))

    filing_date = db.Column("date_filed",  db.Date,         nullable=False, index=True)
    city        = db.Column("principal_city", db.String(255), index=True)

    county = db.Column(db.String(255), index=True)
    state = db.Column(db.String(50), default="FL", index=True)
    registered_agent = db.Column(db.String(255))
    doc_number = db.Column(db.String(100), unique=True)  # idempotent upsert target
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    principal_address = db.Column(db.Text)
    mailing_address   = db.Column(db.Text)
    fei_ein               = db.Column(db.String(32))
    effective_date        = db.Column(db.Date)
    last_event            = db.Column(db.String(100))
    event_date_filed      = db.Column(db.Date)
    event_effective_date  = db.Column(db.Date)
    registered_agent_address = db.Column(db.Text)
    officers_json         = db.Column(db.Text)

    @property
    def officers(self):
        try:
            return json.loads(self.officers_json) if self.officers_json else []
        except Exception:
            return []

    __table_args__ = (
        Index("ix_entities_state_county_date", "state", "county", "date_filed"),
        Index("ix_entities_state_city_date", "state", "principal_city", "date_filed"),
        Index("ix_entities_state_date",      "state", "date_filed"),
    )


class Stat(db.Model):
    __tablename__ = "stats"
    id = db.Column(db.Integer, primary_key=True)
    jurisdiction_id = db.Column(db.Integer, db.ForeignKey("jurisdictions.id"), nullable=False)
    day = db.Column(db.Date, nullable=False)  # e.g., 2025-09-19
    count_day = db.Column(db.Integer, nullable=False, default=0)
    count_mtd = db.Column(db.Integer, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    jurisdiction = db.relationship("Jurisdiction", backref="stats")

    __table_args__ = (
        UniqueConstraint("jurisdiction_id", "day", name="uq_stats_jur_day"),
        Index("ix_stats_jur_day", "jurisdiction_id", "day"),
    )

class Subscription(db.Model):
    __tablename__ = "subscriptions"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=False)
    plan = db.Column(db.String(50), nullable=False)  # e.g. basic, pro
    scope_json = db.Column(db.Text)  # JSON describing scope (state/county)
    api_key_hash = db.Column(db.String(64), unique=True)
    status = db.Column(db.String(50), default="active")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Delivery(db.Model):
    __tablename__ = "deliveries"
    id = db.Column(db.Integer, primary_key=True)
    subscription_id = db.Column(db.Integer, db.ForeignKey("subscriptions.id"))
    run_date = db.Column(db.Date, nullable=False)
    file_url = db.Column(db.String(255))
    status = db.Column(db.String(50), default="pending")
    subscription = db.relationship("Subscription", backref="deliveries")

class Subscriber(db.Model):
    __tablename__ = "subscribers"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255))
    stripe_customer_id = db.Column(db.String(64), unique=True)
    stripe_subscription_id = db.Column(db.String(64), unique=True)
    active = db.Column(db.Boolean, default=True, index=True)
    current_period_end = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
