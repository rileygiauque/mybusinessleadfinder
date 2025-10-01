# nbp/utils.py
import requests
import os
from datetime import datetime

def send_telegram_notification(user_data):
    """
    Send a Telegram notification with user form data
    """
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️  Telegram credentials not configured")
        return False
    
    # Format the message
    counties_display = user_data.get('counties', 'N/A')
    if counties_display == 'florida':
        counties_display = 'All of Florida'
    
    message = f"""
🆕 <b>New Form Submission - NewBizPulse</b>

📧 <b>Email:</b> {user_data.get('email', 'N/A')}
📱 <b>Phone:</b> {user_data.get('phone', 'Not provided')}
🏛️ <b>State:</b> {user_data.get('state', 'Florida')}
📍 <b>Counties:</b> {counties_display}
💳 <b>Plan:</b> {user_data.get('plan_name', 'N/A')}

⏰ <b>Submitted:</b> {user_data.get('timestamp', 'N/A')}
    """
    
    telegram_api = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    try:
        response = requests.post(telegram_api, data={
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        })
        
        if response.status_code == 200:
            print("✅ Telegram notification sent successfully")
            return True
        else:
            print(f"❌ Telegram notification failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error sending Telegram notification: {str(e)}")
        return False
