#!/usr/bin/env python3
"""Add population column if it doesn't exist"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbp import create_app
from nbp.models import db

app = create_app()

with app.app_context():
    try:
        # Check if column exists
        result = db.engine.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='jurisdictions' AND column_name='population'"
        )
        
        if result.rowcount == 0:
            print("Adding population column...")
            db.engine.execute(
                "ALTER TABLE jurisdictions ADD COLUMN population INTEGER DEFAULT 0"
            )
            print("✅ Column added")
        else:
            print("✅ Population column already exists")
    except Exception as e:
        print(f"Error: {e}")
