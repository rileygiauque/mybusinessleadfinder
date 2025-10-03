# nbp/billing.py
import os, json, stripe
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, session, url_for, abort, current_app
from .models import db, Subscriber, Subscription
from werkzeug.routing import BuildError
from .utils import send_telegram_notification

bp = Blueprint("billing", __name__)

# --- ENVIRONMENT VARIABLES (PRODUCTION-READY) ---
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
PRICE_ID_LOCAL = os.getenv("STRIPE_PRICE_LOCAL")
PRICE_ID_REGIONAL = os.getenv("STRIPE_PRICE_REGIONAL")
PRICE_ID_STATEWIDE = os.getenv("STRIPE_PRICE_STATEWIDE")

if not STRIPE_SECRET_KEY:
    raise ValueError("STRIPE_SECRET_KEY environment variable not set")

PRICE_ALIAS_TO_ID = {
    "local":     PRICE_ID_LOCAL,
    "regional":  PRICE_ID_REGIONAL,
    "statewide": PRICE_ID_STATEWIDE,
}

def use_stripe():
    """
    Use Stripe key from environment variables.
    """
    stripe.api_key = STRIPE_SECRET_KEY
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

        # Map Stripe product ID to plan name using env variables
        PLAN_ID_TO_NAME = {
            PRICE_ID_LOCAL: "Local Star",
            PRICE_ID_REGIONAL: "Regional Hero",
            PRICE_ID_STATEWIDE: "Statewide Boss",
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

        base = os.getenv("APP_BASE_URL") or request.url_root.rstrip('/')
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

        base = os.getenv("APP_BASE_URL") or request.url_root.rstrip('/')
        
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

        # ✅ Check the "kind" field to determine plan type
        kind = scope.get('kind', '')
        slugs = scope.get('slugs', [])

        # Determine redirect based on scope structure
        if kind == 'state' or scope.get('slug') == 'florida':
            # Statewide plan
            redirect_url = base + "/new-business/florida/"
        elif kind == 'counties' and len(slugs) == 1:
            # Single county - Local Star plan
            redirect_url = base + f"/new-business/florida/county/{slugs[0]}/"
        elif kind == 'counties' and len(slugs) > 1:
            # Multi-county - Regional Hero plan (redirect to first county)
            redirect_url = base + f"/new-business/florida/county/{slugs[0]}/"
        else:
        # Fallback (legacy data or missing scope)
            redirect_url = base + "/new-business/florida/"
        
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

    base = os.getenv("APP_BASE_URL") or request.url_root.rstrip('/')

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

    checkout_session = s.checkout.Session.retrieve(
        session_id,
        expand=["customer", "subscription", "line_items"],
    )

    session["is_subscriber"] = True
    
    if checkout_session.get("customer"):
        session["stripe_customer_id"] = checkout_session["customer"]

    meta = (checkout_session.get("metadata") or {})
    stored_email = meta.get("nbp_email")
    
    if not stored_email:
        customer_details = checkout_session.get("customer_details") or {}
        stored_email = customer_details.get("email")
    
    if stored_email:
        session["user_email"] = stored_email
        current_app.logger.info(f"✅ Set user_email in session: {stored_email}")

    plan = (meta.get("nbp_plan") or "").lower()
    counties_csv = (meta.get("nbp_counties") or "").strip()
    counties = [c for c in counties_csv.split(",") if c]
    
    # ✅ Store counties in session for profile display
    if counties:
        session["selected_counties"] = counties
        current_app.logger.info(f"✅ Stored counties in session: {counties}")

    def compute_redirect(plan: str, counties: list[str]) -> str:
        if plan == "statewide" or "florida" in counties:
            return "/new-business/florida/"
        if len(counties) == 1:
            return f"/new-business/florida/county/{counties[0]}/"
        return "/new-business/florida/multi/?counties=" + ",".join(counties)

    redirect_to = compute_redirect(plan, counties)
    current_app.logger.info(f"✅ Redirecting to: {redirect_to}")
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
                            subscription_status='active',
                            trial_end_date=None
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

            # ✅ ADD TELEGRAM NOTIFICATION HERE (after user is created)
            stored_email = metadata.get("nbp_email") or email
            counties = metadata.get("nbp_counties", "")
            plan_name = metadata.get("nbp_plan_name", metadata.get("nbp_plan", "Unknown"))

            # Extract phone from custom fields
            phone = None
            for f in (data.get("custom_fields") or []):
                if f.get("key") == "buyer_phone":
                    phone = (f.get("text") or {}).get("value")
                    break

            # Send Telegram notification for paid subscription
            send_telegram_notification({
                'email': stored_email,
                'phone': phone or 'Not provided',
                'state': 'Florida',
                'counties': counties,
                'plan_name': plan_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'source': '💳 Stripe Checkout'
            })

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
    
                # ✅ Build scope_json in the format views.py expects
                scope_json = None
                if counties_csv:
                    # Split counties and build proper structure
                    county_list = [c.strip() for c in counties_csv.split(',') if c.strip()]
        
                    # Check if it's statewide
                    if 'florida' in county_list or len(county_list) == 67:  # all Florida counties
                        scope_json = json.dumps({
                            "kind": "state",
                            "slug": "florida"
                        })
                    else:
                        scope_json = json.dumps({
                            "kind": "counties",
                            "slugs": county_list  # ✅ Changed from string to list
                        })
                elif scope:
                    scope_json = json.dumps({"jurisdiction": scope})

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
            from .models import User
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
                        if status in ("trialing", "active"):
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
        
        # ✅ ADD THESE 6 LINES HERE ✅
        # Ensure cust_id is a string, not a dict
        if isinstance(cust_id, dict):
            cust_id = cust_id.get('id')
            current_app.logger.warning(f"   ⚠️ Customer ID was a dict, extracted ID: {cust_id}")
            # Update session with correct string value
            session["stripe_customer_id"] = cust_id
        
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
        
        # ✅ ADD THESE 4 LINES HERE ✅
        # Validate customer ID format before sending to Stripe
        if not isinstance(cust_id, str) or not cust_id.startswith('cus_'):
            current_app.logger.error(f"❌ Invalid customer ID format: {cust_id}")
            return redirect(url_for("public.home") + "#plans")
        
        base = os.getenv("APP_BASE_URL") or request.url_root.rstrip('/')
        
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
