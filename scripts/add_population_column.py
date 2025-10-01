#!/usr/bin/env python3
"""Add population column if it doesn't exist (SQLAlchemy 2.0 compatible)"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbp import create_app
from nbp.models import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        # Check if column exists (SQLAlchemy 2.0 way)
        with db.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='jurisdictions' AND column_name='population'"
            ))
            
            exists = result.fetchone() is not None
            
            if not exists:
                print("Adding population column to jurisdictions table...")
                conn.execute(text(
                    "ALTER TABLE jurisdictions ADD COLUMN population INTEGER DEFAULT 0"
                ))
                conn.commit()
                print("✅ Population column added successfully")
            else:
                print("✅ Population column already exists")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
