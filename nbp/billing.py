# nbp/billing.py
import os, json, stripe
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, session, url_for, abort, current_app
from .models import db, Subscriber, Subscription
from werkzeug.routing import BuildError

bp = Blueprint("billing", __name__)

# --- HARD-CODED (TEST) ---
STRIPE_SECRET_KEY_HARDCODED = "sk_test_51GbFkxE4vPtJSn6ecxnMRVMsiWwgJrxOWxsQ7rSuA1s5NtTYEyRKyNr7Bi575PlHwTqng7gcI92coYA5UpBwu0xT00f24K6saO"  # test secret key

PRICE_ID_LOCAL     = "prod_T7xfmhXxq0Ad5U"
PRICE_ID_REGIONAL  = "prod_T7xgzN8li2Irrr"
PRICE_ID_STATEWIDE = "prod_T7xgk8o1xOdQaP"

PRICE_ALIAS_TO_ID = {
    "local":     PRICE_ID_LOCAL,
    "regional":  PRICE_ID_REGIONAL,
    "statewide": PRICE_ID_STATEWIDE,
}

def use_stripe():
    """
    Always use the hard-coded test key. No env, no guessing.
    """
    stripe.api_key = STRIPE_SECRET_KEY_HARDCODED
    return stripe




def _upsert_subscriber(*, customer_id, subscription_id, email=None, active=True, current_period_end=None):
    s = (Subscriber.query
         .filter((Subscriber.stripe_customer_id == customer_id) |
                 (Subscriber.stripe_subscription_id == subscription_id))
         .first())
    if not s:
        s = Subscriber(
            email=email,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
        )
    else:
        if email and not s.email:
            s.email = email
        if s.stripe_customer_id != customer_id:
            s.stripe_customer_id = customer_id
        if s.stripe_subscription_id != subscription_id:
            s.stripe_subscription_id = subscription_id

    s.active = bool(active)
    s.current_period_end = current_period_end
    db.session.add(s)
    db.session.commit()
    return s

# Add this to billing.py

@bp.post("/register")
def register():
    """
    Validate registration and redirect to Stripe checkout.
    User account will be created AFTER successful payment via webhook.
    """
    try:
        data = request.get_json()
        email = data.get("email", "").strip()
        password = data.get("password", "")
        plan_id = data.get("plan_id", "").strip()
        counties = data.get("counties", "").strip()

        if not email or not password or not plan_id:
            return {"success": False, "error": "Missing required fields"}, 400

        # Basic email validation
        from .models import User
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return {"success": False, "error": "Email already registered"}, 400

        # Map Stripe product ID to plan name
        PLAN_ID_TO_NAME = {
            "prod_T7xfmhXxq0Ad5U": "Local Star",
            "prod_T7xgzN8li2Irrr": "Regional Hero",
            "prod_T7xgk8o1xOdQaP": "Statewide Boss",
        }
        
        plan_name = PLAN_ID_TO_NAME.get(plan_id)
        if not plan_name:
            return {"success": False, "error": "Invalid plan"}, 400

        # Create Stripe checkout session
        s = use_stripe()
        
        # Resolve plan_id to price
        resolved = plan_id
        if resolved.startswith("prod_"):
            product = s.Product.retrieve(resolved, expand=["default_price"])
            default_price = product.get("default_price")
            if isinstance(default_price, dict):
                selected_price = default_price.get("id")
            else:
                selected_price = default_price
            plan_label = product.get("name", plan_name)
        else:
            return {"success": False, "error": f"Invalid plan_id format: {plan_id}"}, 400

        # Store password hash temporarily in metadata (we'll use it in webhook)
        from werkzeug.security import generate_password_hash
        password_hash = generate_password_hash(password, method='pbkdf2:sha256')

        base = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")
        session_obj = s.checkout.Session.create(
            mode="subscription",
            customer_email=email,
            line_items=[{"price": selected_price, "quantity": 1}],
            allow_promotion_codes=True,
            billing_address_collection="auto",
            custom_fields=[{
                "key": "buyer_phone",
                "label": {"type": "custom", "custom": "Phone"},
                "type": "text",
                "optional": False
            }],
            subscription_data={
                "trial_period_days": 30,
                "metadata": {
                    "nbp_plan": plan_label,
                    "nbp_counties": counties,
                    "nbp_email": email,  # Store for webhook
                    "nbp_password_hash": password_hash,  # Store hashed password
                    "nbp_plan_name": plan_name,  # Store plan name for DB lookup
                },
            },
            metadata={
                "nbp_plan": plan_label,
                "nbp_counties": counties,
                "nbp_email": email,
                "nbp_password_hash": password_hash,
                "nbp_plan_name": plan_name,
            },
            success_url=url_for("billing.subscribe_success", _external=True) + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=url_for("billing.subscribe_cancel", _external=True),
        )

        return {
            "success": True,
            "stripe_url": session_obj.url
        }

    except Exception as e:
        current_app.logger.error(f"Registration error: {e}")
        return {"success": False, "error": "Registration failed. Please try again."}, 500

@bp.post("/login")
def login():
    """
    Authenticate user and redirect to their nb_page based on subscription.
    Checks users table for accounts created through pricing flow.
    """
    try:
        from .models import User, Subscription
        from werkzeug.security import check_password_hash
        
        data = request.get_json()
        email = data.get("email", "").strip()
        password = data.get("password", "")
        
        if not email or not password:
            return {"success": False, "error": "Email and password required"}, 400
        
        # Find user in users table
        user = User.query.filter_by(email=email).first()
        
        if not user:
            # User doesn't exist yet - they need to sign up through a pricing plan
            return {"success": False, "error": "No account found. Please sign up through a pricing plan first."}, 401
        
        # Check password
        if not check_password_hash(user.password_hash, password):
            return {"success": False, "error": "Invalid email or password"}, 401
        
        # Check if user has active subscription
        if user.subscription_status not in ('active', 'trialing'):
            return {"success": False, "error": "Your subscription is not active"}, 403
        
        # Set session
        session['is_subscriber'] = True
        session['user_email'] = user.email
        session['user_id'] = user.id
        
        # Find their subscription to determine redirect
        subscription = Subscription.query.filter_by(
            email=email,
            status='active'
        ).order_by(Subscription.created_at.desc()).first()
        
        if not subscription:
            # No active subscription found, default to Florida page
            return {
                "success": True,
                "redirect_url": "/new-business/florida/"
            }
        
        # Parse their subscription scope to determine redirect
        import json
        scope = {}
        if subscription.scope_json:
            try:
                scope = json.loads(subscription.scope_json)
            except:
                pass
        
        # Get counties from scope_json
        counties_csv = scope.get('counties', '')
        counties = [c.strip() for c in counties_csv.split(',') if c.strip()]
        
        # Determine redirect based on plan
        plan_name = subscription.plan.lower()
        
        if 'statewide' in plan_name or 'florida' in counties:
            redirect_url = "/new-business/florida/"
        elif len(counties) == 1:
            # Single county - Local Star plan
            redirect_url = f"/new-business/florida/county/{counties[0]}/"
        elif len(counties) > 1:
            # Multi-county - Regional Hero plan
            redirect_url = f"/new-business/florida/county/{counties[0]}/"
        else:
            # Fallback
            redirect_url = "/new-business/florida/"
        
        return {
            "success": True,
            "redirect_url": redirect_url
        }
        
    except Exception as e:
        current_app.logger.error(f"Login error: {e}")
        return {"success": False, "error": "Login failed. Please try again."}, 500
    
@bp.get("/logout")
def logout():
    """
    Log out user and clear session.
    """
    session.clear()
    return redirect(url_for('public.home'))

@bp.get("/subscribe")
def subscribe():
    tpl = os.path.join(os.path.dirname(__file__), "templates", "subscribe.html")
    if not os.path.exists(tpl):
        return """<h1>Subscribe</h1>
                  <form action='/create-checkout-session' method='POST'>
                    <button>$99/month — Continue</button>
                  </form>"""
    return render_template("subscribe.html")

@bp.get("/plans")
def plans():
    # Standalone pricing page
    return render_template("billing_plans.html")

@bp.post("/create-checkout-session")
def create_checkout_session():
    s = use_stripe()

    """
    Starts a Stripe Checkout session for a subscription with a 30-day trial.
    Accepts:
      - price_id = 'local' | 'regional' | 'statewide'  (aliases -> your prod_* ids)
      - price_id = 'prod_...'                           (product id -> resolve to default price)
      - price_id = 'price_...'                          (raw price id)
    Optional:
      - jurisdiction = slug saved in metadata
    """

    # inputs
    alias_or_id = (request.form.get("price_id") or "").strip().lower()
    jur_slug    = (request.form.get("jurisdiction") or "").strip()
    if not alias_or_id:
        return "Missing price_id", 400

    # CSV of areas from the form (e.g., "miami-dade,broward" or "florida")
    counties_csv = (request.form.get("counties") or "").strip()

    # resolve alias -> id
    resolved = PRICE_ALIAS_TO_ID.get(alias_or_id, request.form.get("price_id").strip())

    # if product id, fetch its default price
    if resolved.startswith("prod_"):
        product = s.Product.retrieve(resolved, expand=["default_price"])
        default_price = product.get("default_price")
        # default_price can be either ID (str) or expanded object
        if isinstance(default_price, dict):
            selected_price = default_price.get("id")
        else:
            selected_price = default_price
        if not selected_price or not str(selected_price).startswith("price_"):
            return "Product has no default price configured in Stripe.", 400
        plan_label = product.get("name") or alias_or_id
    else:
        # raw price id path
        if not resolved.startswith("price_"):
            return "price_id must be an alias (local/regional/statewide), a prod_ id, or a price_ id.", 400
        selected_price = resolved
        # try to infer plan label from price’s product name (optional)
        try:
            price_obj = s.Price.retrieve(selected_price, expand=["product"])
            prod = price_obj.get("product")
            plan_label = (prod.get("name") if isinstance(prod, dict) else selected_price) or selected_price
        except Exception:
            plan_label = selected_price

    base = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")

    # create checkout session with 30-day trial
    session_obj = s.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": selected_price, "quantity": 1}],
        allow_promotion_codes=True,
        billing_address_collection="auto",


        custom_fields=[{
            "key": "buyer_phone",
            "label": {"type": "custom", "custom": "Phone"},
            "type": "text",
            "optional": False
        }],


        subscription_data={
            "trial_period_days": 30,
            "metadata": {
                "nbp_plan": plan_label,
                "nbp_jurisdiction": jur_slug,
                "nbp_counties": counties_csv,
            },
        },
        metadata={
            "nbp_plan": plan_label,
            "nbp_jurisdiction": jur_slug,
            "nbp_counties": counties_csv,
        },
        success_url=url_for("billing.subscribe_success", _external=True) + "?session_id={CHECKOUT_SESSION_ID}",
        cancel_url=url_for("billing.subscribe_cancel",  _external=True),
        
    )

    return redirect(session_obj.url, code=303)

@bp.get("/subscribe/success")
def subscribe_success():
    s = use_stripe()

    session_id = request.args.get("session_id")
    if not session_id:
        return abort(400, description="Missing session_id")

    # Pull the session so we can read metadata
    checkout_session = s.checkout.Session.retrieve(
        session_id,
        expand=["customer", "subscription", "line_items"],
    )

    # Mark as subscriber
    session["is_subscriber"] = True
    
    # Store customer ID for billing portal
    if checkout_session.get("customer"):
        session["stripe_customer_id"] = checkout_session["customer"]

    # ✅ GET EMAIL FROM CHECKOUT SESSION AND SET IN SESSION
    meta = (checkout_session.get("metadata") or {}) if isinstance(checkout_session, dict) else {}
    stored_email = meta.get("nbp_email")
    
    # Also check customer_details as fallback
    if not stored_email:
        customer_details = checkout_session.get("customer_details") or {}
        stored_email = customer_details.get("email")
    
    if stored_email:
        session["user_email"] = stored_email  # ✅ SET THIS!
        current_app.logger.info(f"✅ Set user_email in session: {stored_email}")

    # ---- Build redirect based on plan + counties
    plan = (meta.get("nbp_plan") or "").lower()
    counties_csv = (meta.get("nbp_counties") or "").strip()
    counties = [c for c in counties_csv.split(",") if c]
    
    # Store counties in session for profile display
    if counties:
        session["selected_counties"] = counties

    def compute_redirect(plan: str, counties: list[str]) -> str:
        # STATEWIDE or explicit florida
        if plan == "statewide" or "florida" in counties:
            return "/new-business/florida/"

        # SINGLE county
        if len(counties) == 1:
            return f"/new-business/florida/county/{counties[0]}/"

        # MULTI (10-county plan) — point to a combined view you implement
        return "/new-business/florida/multi/?counties=" + ",".join(counties)

    redirect_to = compute_redirect(plan, counties)
    return redirect(redirect_to)


@bp.post("/stripe/webhook")
def stripe_webhook():
    s = use_stripe()
    
    endpoint_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    if not endpoint_secret:
        return "Webhook secret not set", 500

    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
    except Exception as e:
        return f"Webhook signature verification failed: {e}", 400

    etype = event["type"]
    data = event["data"]["object"]

    try:
        if etype == "checkout.session.completed" and data.get("mode") == "subscription":
            customer_id = data.get("customer")
            subscription_id = data.get("subscription")
            email = (data.get("customer_details") or {}).get("email")
            
            metadata = data.get("metadata") or {}
            
            # ✅ CREATE USER HERE (after payment initiated)
            from .models import User, Plan
            
            stored_email = metadata.get("nbp_email")
            password_hash = metadata.get("nbp_password_hash")
            plan_name = metadata.get("nbp_plan_name")
            counties = metadata.get("nbp_counties")
            
            if stored_email and password_hash and plan_name:
                # Check if user already exists
                existing_user = User.query.filter_by(email=stored_email).first()
                
                if not existing_user:
                    # Find the plan
                    plan = Plan.query.filter_by(name=plan_name).first()
                    
                    if plan:
                        # Get subscription details
                        sub = s.Subscription.retrieve(subscription_id)
                        trial_end = None
                        if sub.get("trial_end"):
                            trial_end = datetime.utcfromtimestamp(sub["trial_end"])
                        
                        # Create the user NOW (after payment started)
                        new_user = User(
                            email=stored_email,
                            password_hash=password_hash,  # Already hashed
                            plan_id=plan.id,
                            subscription_status='trialing',  # 30-day trial
                            trial_end_date=trial_end
                        )
                        db.session.add(new_user)
                        db.session.commit()
                        current_app.logger.info(f"✅ User created via webhook: {stored_email}")
                    else:
                        current_app.logger.error(f"❌ Plan '{plan_name}' not found in database")
                else:
                    # User already exists, update their subscription status
                    sub = s.Subscription.retrieve(subscription_id)
                    trial_end = None
                    if sub.get("trial_end"):
                        trial_end = datetime.utcfromtimestamp(sub["trial_end"])
                    
                    existing_user.subscription_status = 'trialing'
                    existing_user.trial_end_date = trial_end
                    db.session.commit()
                    current_app.logger.info(f"✅ User updated via webhook: {stored_email}")

            # Extract phone from custom fields
            phone = None
            for f in (data.get("custom_fields") or []):
                if f.get("key") == "buyer_phone":
                    phone = (f.get("text") or {}).get("value")
                    break
            if customer_id and phone:
                try:
                    s.Customer.modify(customer_id, phone=phone)
                except Exception:
                    pass

            # Get subscription details
            sub = s.Subscription.retrieve(subscription_id)
            cpe = datetime.utcfromtimestamp(sub["current_period_end"]) if sub.get("current_period_end") else None
            status = sub.get("status")
            active = status in ("trialing", "active")

            _upsert_subscriber(
                customer_id=customer_id,
                subscription_id=subscription_id,
                email=email or stored_email,
                active=active,
                current_period_end=cpe,
            )

            # Mirror the success handler: create/update a Subscription row
            try:
                plan_label = metadata.get("nbp_plan") or "unknown"
                scope = metadata.get("nbp_jurisdiction") or ""
                counties_csv = metadata.get("nbp_counties") or ""
                
                # Build scope_json with counties
                if counties_csv:
                    scope_json = json.dumps({
                        "jurisdiction": scope,
                        "counties": counties_csv
                    })
                elif scope:
                    scope_json = json.dumps({"jurisdiction": scope})
                else:
                    scope_json = None

                existing = None
                if email or stored_email:
                    existing = (Subscription.query
                                .filter_by(email=email or stored_email, plan=plan_label)
                                .order_by(Subscription.created_at.desc())
                                .first())
                if not existing:
                    existing = Subscription(email=email or stored_email or "", plan=plan_label)

                if scope_json:
                    existing.scope_json = scope_json
                existing.status = "active" if active else (status or "inactive")
                db.session.add(existing)
                db.session.commit()
            except Exception as e:
                current_app.logger.error(f"Error creating Subscription record: {e}")

        elif etype in ("customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"):
            sub = data
            customer_id = sub.get("customer")
            subscription_id = sub.get("id")
            cpe = datetime.utcfromtimestamp(sub.get("current_period_end")) if sub.get("current_period_end") else None
            status = sub.get("status")
            active = status in ("trialing", "active")

            _upsert_subscriber(
                customer_id=customer_id,
                subscription_id=subscription_id,
                active=active,
                current_period_end=cpe,
            )

            # Update User subscription_status based on subscription changes
            try:
                metadata = sub.get("metadata") or {}
                stored_email = metadata.get("nbp_email")
                
                if stored_email:
                    user = User.query.filter_by(email=stored_email).first()
                    if user:
                        # Map Stripe status to our status
                        if status == "trialing":
                            user.subscription_status = "trialing"
                        elif status == "active":
                            user.subscription_status = "active"
                        elif status in ("canceled", "unpaid", "past_due"):
                            user.subscription_status = "inactive"
                        
                        db.session.commit()
                        current_app.logger.info(f"✅ User subscription status updated: {stored_email} -> {status}")
            except Exception as e:
                current_app.logger.error(f"Error updating User subscription status: {e}")

            # Keep a Subscription row in sync by customer+plan metadata if present
            try:
                plan_label = (sub.get("metadata") or {}).get("nbp_plan") or "unknown"

                # If you maintain a customer->email map, use it here; otherwise best-effort via Subscriber
                subscriber = Subscriber.query.filter_by(stripe_customer_id=customer_id).first()
                if subscriber and subscriber.email:
                    existing = (Subscription.query
                                .filter_by(email=subscriber.email, plan=plan_label)
                                .order_by(Subscription.created_at.desc())
                                .first())
                    if existing:
                        existing.status = "active" if active else (status or "inactive")
                        db.session.add(existing)
                        db.session.commit()
            except Exception as e:
                current_app.logger.error(f"Error updating Subscription record: {e}")
                
    except Exception as e:
        current_app.logger.error(f"Webhook processing error: {e}")
        return f"Processed with warning: {e}", 200

    return "", 200

@bp.get("/subscribe/cancel")
def subscribe_cancel():
    # Send them somewhere sensible—home or pricing
    return redirect(url_for("index"))  # change if your homepage endpoint differs

@bp.get("/billing/portal")
def billing_portal():
    """
    Customer self-serve portal for managing subscription, payment, and upgrades.
    """
    current_app.logger.info(f"🔍 Billing portal accessed")
    current_app.logger.info(f"   is_subscriber: {session.get('is_subscriber')}")
    current_app.logger.info(f"   user_email: {session.get('user_email')}")
    current_app.logger.info(f"   stripe_customer_id in session: {session.get('stripe_customer_id')}")
    
    if not session.get("is_subscriber"):
        current_app.logger.warning("❌ Not a subscriber, redirecting to home")
        return redirect(url_for("public.home"))
    
    try:
        s = use_stripe()
        
        # Try to get customer_id from session first
        cust_id = session.get("stripe_customer_id")
        current_app.logger.info(f"   Customer ID from session: {cust_id}")
        
        # If not in session, look it up from database
        if not cust_id and session.get("user_email"):
            current_app.logger.info(f"   Looking up in DB for email: {session.get('user_email')}")
            from .models import Subscriber
            subscriber = Subscriber.query.filter_by(
                email=session["user_email"],
                active=True
            ).first()
            
            if subscriber:
                current_app.logger.info(f"   Found subscriber: {subscriber.email}, customer_id: {subscriber.stripe_customer_id}")
                if subscriber.stripe_customer_id:
                    cust_id = subscriber.stripe_customer_id
                    session["stripe_customer_id"] = cust_id  # Cache it
                    current_app.logger.info(f"   ✅ Cached customer_id in session: {cust_id}")
            else:
                current_app.logger.warning(f"   ❌ No subscriber found in DB")
        
        if not cust_id:
            current_app.logger.error("❌ No customer ID found, redirecting to plans")
            return redirect(url_for("public.home") + "#plans")
        
        base = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")
        
        # Create billing portal session
        current_app.logger.info(f"   ✅ Creating portal for customer: {cust_id}")
        portal = s.billing_portal.Session.create(
            customer=cust_id,
            return_url=base + "/new-business/florida/"
        )
        
        current_app.logger.info(f"   ✅ Redirecting to portal: {portal.url}")
        return redirect(portal.url, code=303)
        
    except Exception as e:
        current_app.logger.error(f"❌ Billing portal error: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return redirect(url_for("public.home") + "#plans")
