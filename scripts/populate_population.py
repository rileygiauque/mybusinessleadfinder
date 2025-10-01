#!/usr/bin/env python3
"""
One-time script to populate jurisdiction populations
Run with: python scripts/populate_population.py
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbp import create_app
from nbp.models import Jurisdiction, db

# Florida county populations (2024 Census estimates)
# NOTE: Slugs match database format with '-county' suffix
POPULATIONS = {
    # Major metros (300k+)
    'miami-dade-county': 2716940,
    'broward-county': 1944375,
    'palm-beach-county': 1496770,
    'hillsborough-county': 1459762,
    'orange-county': 1429908,
    
    # Large (100k-300k)
    'duval-county': 995567,
    'pinellas-county': 959107,
    'lee-county': 822152,
    'polk-county': 787404,
    'brevard-county': 606612,
    'pasco-county': 590551,
    'volusia-county': 553543,
    'seminole-county': 471826,
    'sarasota-county': 434263,
    'manatee-county': 410242,
    'collier-county': 384902,
    'lake-county': 383956,
    'st-lucie-county': 329226,
    'escambia-county': 324603,
    'leon-county': 295103,
    'marion-county': 375908,
    'osceola-county': 402134,
    'st-johns-county': 273425,
    'clay-county': 226718,
    'charlotte-county': 188910,
    'hernando-county': 194515,
    
    # Medium (50k-100k)
    'alachua-county': 278468,
    'bay-county': 182845,
    'citrus-county': 153843,
    'flagler-county': 116626,
    'indian-river-county': 163902,
    'martin-county': 161824,
    'okaloosa-county': 211668,
    'santa-rosa-county': 192783,
    'sumter-county': 134732,
    'nassau-county': 94046,
    'walton-county': 75305,
    'monroe-county': 82086,
    'putnam-county': 73321,
    'highlands-county': 103268,
    
    # Small (25k-50k)
    'columbia-county': 69698,
    'desoto-county': 37010,
    'gadsden-county': 45087,
    'hardee-county': 26938,
    'hendry-county': 42022,
    'jackson-county': 46852,
    'levy-county': 42915,
    'okeechobee-county': 41611,
    'suwannee-county': 45629,
    'wakulla-county': 33764,
    'washington-county': 25318,
    'baker-county': 28259,
    'bradford-county': 27440,
    
    # Very small (0-25k)
    'calhoun-county': 13648,
    'dixie-county': 16759,
    'franklin-county': 12451,
    'gilchrist-county': 18439,
    'glades-county': 13125,
    'gulf-county': 14192,
    'hamilton-county': 14004,
    'holmes-county': 19653,
    'jefferson-county': 14145,
    'lafayette-county': 8226,
    'liberty-county': 7974,
    'madison-county': 18732,
    'taylor-county': 21796,
    'union-county': 15766,
}

def main():
    app = create_app()
    
    with app.app_context():
        updated = 0
        missing = []
        
        print("Updating county populations...")
        print("-" * 50)
        
        for slug, pop in sorted(POPULATIONS.items(), key=lambda x: x[1], reverse=True):
            jur = Jurisdiction.query.filter_by(slug=slug, kind='county').first()
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
