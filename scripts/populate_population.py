#!/usr/bin/env python3
"""
One-time script to populate jurisdiction populations
Run with: python scripts/populate_population.py
"""

import sys
import os

# Add parent directory to path so we can import app
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbp import create_app
from nbp.models import Jurisdiction, db

# Florida county populations (2024 Census estimates)
POPULATIONS = {
    # Major metros (300k+)
    'miami-dade': 2716940,
    'broward': 1944375,
    'palm-beach': 1496770,
    'hillsborough': 1459762,
    'orange': 1429908,
    
    # Large (100k-300k)
    'duval': 995567,
    'pinellas': 959107,
    'lee': 822152,
    'polk': 787404,
    'brevard': 606612,
    'pasco': 590551,
    'volusia': 553543,
    'seminole': 471826,
    'sarasota': 434263,
    'manatee': 410242,
    'collier': 384902,
    'lake': 383956,
    'st-lucie': 329226,
    'escambia': 324603,
    'leon': 295103,
    'marion': 375908,
    'osceola': 402134,
    'st-johns': 273425,
    'clay': 226718,
    'charlotte': 188910,
    'hernando': 194515,
    
    # Medium (50k-100k)
    'alachua': 278468,
    'bay': 182845,
    'citrus': 153843,
    'flagler': 116626,
    'indian-river': 163902,
    'martin': 161824,
    'okaloosa': 211668,
    'santa-rosa': 192783,
    'sumter': 134732,
    'nassau': 94046,
    'walton': 75305,
    'monroe': 82086,
    'putnam': 73321,
    'highlands': 103268,
    
    # Small (25k-50k)
    'columbia': 69698,
    'desoto': 37010,
    'gadsden': 45087,
    'hardee': 26938,
    'hendry': 42022,
    'jackson': 46852,
    'levy': 42915,
    'okeechobee': 41611,
    'suwannee': 45629,
    'wakulla': 33764,
    'washington': 25318,
    'baker': 28259,
    'bradford': 27440,
    
    # Very small (0-25k)
    'calhoun': 13648,
    'dixie': 16759,
    'franklin': 12451,
    'gilchrist': 18439,
    'glades': 13125,
    'gulf': 14192,
    'hamilton': 14004,
    'holmes': 19653,
    'jefferson': 14145,
    'lafayette': 8226,
    'liberty': 7974,
    'madison': 18732,
    'taylor': 21796,
    'union': 15766,
}

def main():
    app = create_app()
    
    with app.app_context():
        updated = 0
        missing = []
        
        print("Updating county populations...")
        print("-" * 50)
        
        for slug, pop in sorted(POPULATIONS.items(), key=lambda x: x[1], reverse=True):
            # Try lowercase slug match first
            jur = Jurisdiction.query.filter(
                Jurisdiction.slug.ilike(slug),  # ← Case-insensitive
                Jurisdiction.kind == 'county'
            ).first()
    
            if jur:
                jur.population = pop
                updated += 1
                print(f"✓ {jur.name:25} {pop:>10,}")
            else:
                missing.append(slug)
                print(f"✗ {slug:25} NOT FOUND")
        
        # Set Florida state population (sum of all counties)
        fl = Jurisdiction.query.filter_by(slug='florida', kind='state').first()
        if fl:
            fl.population = sum(POPULATIONS.values())
            updated += 1
            print("-" * 50)
            print(f"✓ {'FLORIDA (TOTAL)':25} {fl.population:>10,}")
        
        # Commit changes
        db.session.commit()
        
        print("\n" + "=" * 50)
        print(f"✅ Updated {updated} jurisdictions")
        
        if missing:
            print(f"⚠️  Missing {len(missing)} counties: {', '.join(missing)}")
        
        # Show counties without population
        empty = Jurisdiction.query.filter_by(kind='county', population=None).count()
        if empty > 0:
            print(f"⚠️  {empty} counties still have NULL population")
        
        print("=" * 50)

if __name__ == '__main__':
    main()
