#!/usr/bin/env python3
"""
Affinity Test Tool - Calculate affinity levels 1-4 for Uma Musume inheritance

Usage:
  python scripts/affinity_test.py <main_id> <left_id> <left_left_id> <left_right_id> [right_id] [right_left_id] [right_right_id] [inheritable_id]

Arguments:
  main_id         - Main character being trained
  left_id         - Legacy 1 (left parent)
  left_left_id    - Sub-Legacy 1-1 (Legacy 1's left parent)
  left_right_id   - Sub-Legacy 1-2 (Legacy 1's right parent)
  right_id        - Optional: Legacy 2 (right parent), use 0 to skip
  right_left_id   - Optional: Sub-Legacy 2-1 (Legacy 2's left parent), use 0 to skip
  right_right_id  - Optional: Sub-Legacy 2-2 (Legacy 2's right parent), use 0 to skip
  inheritable_id  - Optional: specific inheritable character to check

Examples:
  python scripts/affinity_test.py 1030 1004 1001 1002                          # Check only Legacy 1 side
  python scripts/affinity_test.py 1030 1004 1001 1002 1020 1003 1005           # Check both sides
  python scripts/affinity_test.py 1030 1004 1001 1002 1020 1003 1005 1026      # Check both sides + inheritable
"""

import sqlite3
import sys
import os
from collections import defaultdict

# Default path to the Uma Musume master DB
SQLITE_PATH = r"C:\Users\lars\AppData\LocalLow\Cygames\Umamusume\master\master.mdb"


def load_data(sqlite_path: str):
    """Load relation data from Uma Musume master.mdb"""
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # relation_type -> relation_point
    cur.execute("SELECT relation_type, relation_point FROM succession_relation")
    rel_points = {int(rt): int(rp) for rt, rp in cur.fetchall()}

    # chara_id -> set(relation_type)
    chara_rel = defaultdict(set)
    cur.execute("SELECT chara_id, relation_type FROM succession_relation_member")
    for chara_id, relation_type in cur.fetchall():
        chara_rel[int(chara_id)].add(int(relation_type))

    # Get character names for display
    cur.execute("SELECT id, text FROM text_data WHERE category = 6")  # Category 6 is character names
    char_names = {int(id): text for id, text in cur.fetchall()}

    conn.close()
    return rel_points, chara_rel, char_names


def get_affinity_level(score: int) -> int:
    """Convert affinity score to level (1-4)"""
    if score >= 110:
        return 4
    elif score >= 60:
        return 3
    elif score >= 10:
        return 2
    else:
        return 1


def get_affinity_symbol(level: int) -> str:
    """Get symbol for affinity level"""
    symbols = {1: "◯", 2: "△", 3: "◎", 4: "◎◎"}
    return symbols.get(level, "?")


def calculate_affinity(main_id: int, left_id: int, 
                       left_left_id: int, left_right_id: int, 
                       right_id: int = 0, 
                       right_left_id: int = 0, right_right_id: int = 0,
                       inheritable_id: int = None,
                       rel_points: dict = None, chara_rel: dict = None, char_names: dict = None):
    """Calculate affinity scores for a given inheritance combination with sub-legacies"""
    
    # Validate that all characters exist
    chars_to_check = [
        (main_id, "main"),
        (left_id, "Legacy 1 (left)"),
        (left_left_id, "Sub-Legacy 1-1"),
        (left_right_id, "Sub-Legacy 1-2")
    ]
    
    # Only check right side if provided
    has_right_side = right_id != 0
    if has_right_side:
        chars_to_check.extend([
            (right_id, "Legacy 2 (right)"),
            (right_left_id, "Sub-Legacy 2-1"),
            (right_right_id, "Sub-Legacy 2-2")
        ])
    
    for char_id, name in chars_to_check:
        if char_id not in chara_rel:
            print(f"❌ Character ID {char_id} ({name}) not found in database")
            return None
    
    if inheritable_id is not None and inheritable_id not in chara_rel:
        print(f"❌ Character ID {inheritable_id} (inheritable) not found in database")
        return None
    
    
    # Build aff2: (from, to) -> score
    def get_aff2(a, b):
        if a == b:
            return 0
        rel_a = chara_rel[a]
        rel_b = chara_rel[b]
        common = rel_a & rel_b
        return sum(rel_points[rt] for rt in common) if common else 0
    
    # Build aff3: (a, b, c) -> score
    def get_aff3(a, b, c):
        if a == b:
            return 0
        rel_a = chara_rel[a]
        rel_b = chara_rel[b]
        ab_common = rel_a & rel_b
        if not ab_common:
            return 0
        rel_c = chara_rel[c]
        common = ab_common & rel_c
        return 0 if a == c else sum(rel_points[rt] for rt in common)
    
    # Get character names
    def get_name(char_id):
        return char_names.get(char_id, f"Character {char_id}")
    
    print(f"\n{'='*80}")
    print(f"Affinity Calculation")
    print(f"{'='*80}")
    print(f"Main Character:      {main_id:4d} - {get_name(main_id)}")
    print(f"Legacy 1 (Left):     {left_id:4d} - {get_name(left_id)}")
    print(f"Sub-Legacy 1-1:      {left_left_id:4d} - {get_name(left_left_id)}")
    print(f"Sub-Legacy 1-2:      {left_right_id:4d} - {get_name(left_right_id)}")
    
    if has_right_side:
        print(f"Legacy 2 (Right):    {right_id:4d} - {get_name(right_id)}")
        print(f"Sub-Legacy 2-1:      {right_left_id:4d} - {get_name(right_left_id)}")
        print(f"Sub-Legacy 2-2:      {right_right_id:4d} - {get_name(right_right_id)}")
    else:
        print(f"Legacy 2 (Right):    (not set)")
    
    # Calculate all components (excluding race affinity as requested)
    print(f"\n{'─'*80}")
    print(f"Affinity Breakdown:")
    print(f"")
    
    # Component 1: Main ↔ Legacy 1
    comp1 = get_aff2(main_id, left_id)
    print(f"  Main Char — Legacy 1:           {comp1:3d} points")
    
    # Component 2: Main ↔ Legacy 1 ∩ Sub-Legacy 1-1
    comp2 = get_aff3(main_id, left_id, left_left_id)
    print(f"  Sub-Legacy 1-1 line:            {comp2:3d} points")
    
    # Component 3: Main ↔ Legacy 1 ∩ Sub-Legacy 1-2
    comp3 = get_aff3(main_id, left_id, left_right_id)
    print(f"  Sub-Legacy 1-2 line:            {comp3:3d} points")
    
    # Only calculate right side if provided
    comp4 = comp5 = comp6 = 0
    race_affinity = 0
    
    if has_right_side:
        # Component 4: Main ↔ Legacy 2
        comp4 = get_aff2(main_id, right_id)
        print(f"  Main Char — Legacy 2:           {comp4:3d} points")
        
        # Component 5: Main ↔ Legacy 2 ∩ Sub-Legacy 2-1
        comp5 = get_aff3(main_id, right_id, right_left_id)
        print(f"  Sub-Legacy 2-1 line:            {comp5:3d} points")
        
        # Component 6: Main ↔ Legacy 2 ∩ Sub-Legacy 2-2
        comp6 = get_aff3(main_id, right_id, right_right_id)
        print(f"  Sub-Legacy 2-2 line:            {comp6:3d} points")
        
        # Component 7: Race affinity (shown but not included in total as requested)
        race_affinity = get_aff2(left_id, right_id)
        print(f"  Legacies' affinity (excluded):  {race_affinity:3d} points")
    else:
        print(f"  (Legacy 2 side not calculated)")
    
    # Total affinity (excluding race affinity)
    total_affinity = comp1 + comp2 + comp3 + comp4 + comp5 + comp6
    
    print(f"\n  {'─'*60}")
    print(f"  Total Affinity (excl. race):   {total_affinity:3d} points")
    
    # Determine affinity level
    affinity_level = get_affinity_level(total_affinity)
    symbol = get_affinity_symbol(affinity_level)
    
    print(f"\n  → Affinity Level: {affinity_level} {symbol}")
    
    # Show what's needed for next level
    if total_affinity < 10:
        needed = 10 - total_affinity
        print(f"     {needed} more points needed for Level 2 (△)")
    elif total_affinity < 60:
        needed = 60 - total_affinity
        print(f"     {needed} more points needed for Level 3 (◎)")
    elif total_affinity < 110:
        needed = 110 - total_affinity
        print(f"     {needed} more points needed for Level 4 (◎◎)")
    else:
        print(f"     Maximum level reached!")
    
    # Show level thresholds
    print(f"\n{'─'*80}")
    print(f"Affinity Level Reference:")
    print(f"  Level 1 (◯):        0 -   9 points")
    print(f"  Level 2 (△):      10 -  59 points")
    print(f"  Level 3 (◎):      60 - 109 points")
    print(f"  Level 4 (◎◎):    110+     points")
    
    # If checking a specific inheritable character
    if inheritable_id is not None:
        print(f"\n{'─'*80}")
        print(f"Inheritable Character Analysis: {inheritable_id:4d} - {get_name(inheritable_id)}")
        print(f"\nNote: This shows the inheritable character's compatibility")
        print(f"with the main character and parent lines.")
        
        # Inheritable components
        inh_comp1 = get_aff2(inheritable_id, main_id)
        inh_comp2 = get_aff3(inheritable_id, main_id, left_id)
        
        inheritable_score = inh_comp1 + inh_comp2
        
        print(f"\n  Inheritable ↔ Main:             {inh_comp1:3d} points")
        print(f"  Inheritable ↔ Main ∩ Legacy 1:  {inh_comp2:3d} points")
        
        if has_right_side:
            inh_comp3 = get_aff3(inheritable_id, main_id, right_id)
            inheritable_score += inh_comp3
            print(f"  Inheritable ↔ Main ∩ Legacy 2:  {inh_comp3:3d} points")
        
        print(f"  {'─'*60}")
        print(f"  Inheritable Total:              {inheritable_score:3d} points")
    
    print(f"{'='*80}\n")


def main():
    if len(sys.argv) < 5:
        print(__doc__)
        sys.exit(1)
    
    try:
        main_id = int(sys.argv[1])
        left_id = int(sys.argv[2])
        left_left_id = int(sys.argv[3])
        left_right_id = int(sys.argv[4])
        
        # Optional right side
        right_id = int(sys.argv[5]) if len(sys.argv) > 5 else 0
        right_left_id = int(sys.argv[6]) if len(sys.argv) > 6 else 0
        right_right_id = int(sys.argv[7]) if len(sys.argv) > 7 else 0
        inheritable_id = int(sys.argv[8]) if len(sys.argv) > 8 else None
    except ValueError:
        print("❌ Error: Character IDs must be integers")
        print(__doc__)
        sys.exit(1)
    
    # Check if master DB exists
    sqlite_path = SQLITE_PATH
    if not os.path.exists(sqlite_path):
        print(f"❌ SQLite file not found: {sqlite_path}")
        print(f"\n💡 Set the correct path by editing SQLITE_PATH in this script")
        sys.exit(1)
    
    # Load data
    print(f"📦 Loading data from master DB...")
    rel_points, chara_rel, char_names = load_data(sqlite_path)
    print(f"✅ Loaded {len(chara_rel)} characters")
    
    # Calculate affinity
    calculate_affinity(main_id, left_id, 
                      left_left_id, left_right_id,
                      right_id, right_left_id, right_right_id,
                      inheritable_id, rel_points, chara_rel, char_names)


if __name__ == "__main__":
    main()
