#!/usr/bin/env python3
import sqlite3
import os
import re
from collections import defaultdict
from datetime import datetime

# Default path to the Uma Musume master DB
SQLITE_PATH = r"C:\Users\lars\AppData\LocalLow\Cygames\Umamusume\master\master.mdb"

# Migration directory
MIGRATIONS_DIR = "migrations"


def get_last_processed_characters():
    """Read affinity_migration.sql to find the full set of character IDs processed.
    
    Returns a set of character IDs from the last migration, or None if no migration exists.
    """
    migration_path = "affinity_migration.sql"
    
    if not os.path.exists(migration_path):
        return None
    
    print(f"   Reading last migration: {migration_path}")
    
    with open(migration_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Look for "-- Character IDs: 1001,1002,..."
        match = re.search(r'--\s+Character IDs:\s+([\d,]+)', content)
        if match:
            return set(int(x) for x in match.group(1).split(',') if x)
        
        # Fallback: old format with only "-- Last character: XXXX"
        match = re.search(r'--\s+Last character:\s+(\d+)', content)
        if match:
            last_char = int(match.group(1))
            # Reconstruct a best-guess set (contiguous range) from old format
            return set(range(1001, last_char + 1))
    
    return None


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

    conn.close()
    return rel_points, chara_rel


def export_saddle_data(sqlite_path: str):
    """Export single_mode_wins_saddle table to JSON"""
    import json
    
    output_path = "data/single_mode_wins_saddle.json"
    
    if not os.path.exists(sqlite_path):
        print(f"❌ SQLite file not found for saddle export: {sqlite_path}")
        return

    print(f"📦 Reading saddle data from: {sqlite_path}")
    
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT * FROM single_mode_wins_saddle")
        rows = cur.fetchall()
        
        saddles = []
        for row in rows:
            saddles.append(dict(row))
            
        # Ensure data directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(saddles, f, indent=2)
            
        print(f"✅ Exported {len(saddles)} saddle definitions to {output_path}")
        
    except sqlite3.Error as e:
        print(f"❌ Failed to export saddle data: {e}")
    finally:
        conn.close()


def compute_affinity_scores(rel_points, chara_rel, max_char_id):
    """Compute affinity scores for all inheritance combinations.
    
    Args:
        rel_points: relation type -> points mapping
        chara_rel: character -> set of relation types mapping
        max_char_id: highest character ID to include in arrays
    
    Returns:
        tuple: (affinity_dict, race_affinity_dict) where:
            affinity_dict: (main, left, right) -> {
                'affinity_scores': [score for chara 1001 to max_char_id],
                'base_affinity': int
            }
            race_affinity_dict: (left, right) -> int (race affinity score)
    """
    chars = sorted(chara_rel.keys())
    
    print(f"   Building affinity lookups...")
    
    # Build aff2: (from, to) -> score
    aff2 = {}
    for a in chars:
        rel_a = chara_rel[a]
        for b in chars:
            if a == b:
                continue
            rel_b = chara_rel[b]
            common = rel_a & rel_b
            if common:
                score = sum(rel_points[rt] for rt in common)
                if score != 0:
                    aff2[(a, b)] = score
    
    # Build aff3: (a, b, c) -> score
    aff3 = {}
    for a in chars:
        rel_a = chara_rel[a]
        for b in chars:
            if a == b:
                continue
            rel_b = chara_rel[b]
            ab_common = rel_a & rel_b
            if not ab_common:
                continue
            for c in chars:
                if c == b:
                    continue
                rel_c = chara_rel[c]
                common = ab_common & rel_c
                score = 0 if a == c else sum(rel_points[rt] for rt in common)
                aff3[(a, b, c)] = score
    
    print(f"   Computing affinity arrays for all inheritance combinations...")
    
    result = {}
    race_affinity_map = {}
    count = 0
    
    for main in chars:
        for left in chars:
            if left == main:
                continue
            for right in chars:
                if right == main or right == left:
                    continue
                
                count += 1
                
                # base_affinity: aff2(main,left) + aff2(main,right)
                # This is what we know when we only have parent 1 (with grandparents 1.1 and 1.2)
                # Triple affinity requires knowing the child (0), which goes in the array
                base_affinity = aff2.get((main, left), 0) + aff2.get((main, right), 0)
                
                # affinity_scores: array indexed by (chara_id - 1001)
                # Array goes from 1001 to max_char_id, filling missing characters with 0
                affinity_array = []
                for chara_id in range(1001, max_char_id + 1):
                    if chara_id not in chara_rel or chara_id == main:
                        affinity_array.append(0)
                    else:
                        score = (aff2.get((chara_id, main), 0) +
                                aff3.get((chara_id, main, left), 0) +
                                aff3.get((chara_id, main, right), 0))
                        affinity_array.append(score)
                
                result[(main, left, right)] = {
                    'affinity_scores': affinity_array,
                    'base_affinity': base_affinity
                }
    
    print(f"   → Generated {count} inheritance combinations")
    return result, race_affinity_map


def export_json(rel_points, chara_rel, max_char_id):
    import json
    output_path = "data/affinity_definitions.json"
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Convert sets to lists for JSON serialization
    chara_rel_list = {str(k): list(v) for k, v in chara_rel.items()}
    
    data = {
        "rel_points": rel_points,
        "chara_rel": chara_rel_list,
        "max_char_id": max_char_id
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"✅ Exported definitions to {output_path}")


def main():
    sqlite_path = SQLITE_PATH
    if not os.path.exists(sqlite_path):
        print(f"❌ SQLite file not found: {sqlite_path}")
        return

    print(f"📦 Reading master DB: {sqlite_path}")
    rel_points, chara_rel = load_data(sqlite_path)
    
    chars = sorted(chara_rel.keys())
    max_char_id = max(chars)
    min_char_id = min(chars)
    
    print(f"📊 Found {len(chars)} characters: {min_char_id} to {max_char_id}")

    # Check last migration to see what characters were previously processed
    print(f"\n🔍 Checking last migration...")
    prev_char_ids = get_last_processed_characters()
    
    if prev_char_ids:
        current_char_ids = set(chars)
        new_char_ids = sorted(current_char_ids - prev_char_ids)
        
        print(f"   → Last migration had {len(prev_char_ids)} characters")
        
        if not new_char_ids:
            print(f"\n✅ Already up to date! No new characters to process.")
            return
        
        print(f"   → {len(new_char_ids)} new character(s) found: {new_char_ids}")
        is_incremental = True
    else:
        print(f"   → No previous affinity migration found")
        print(f"   → Will generate full initialization")
        new_char_ids = []
        is_incremental = False

    # Compute all affinity scores
    print(f"\n📊 Computing affinity scores...")
    affinity_data, race_affinity = compute_affinity_scores(rel_points, chara_rel, max_char_id)

    # Export JSON definitions for Node.js app
    print(f"\n📦 Exporting JSON definitions...")
    export_json(rel_points, chara_rel, max_char_id)
    export_saddle_data(sqlite_path)

    # Always write to the same file - will be applied manually in production
    migration_path = f"affinity_migration.sql"

    print(f"\n📝 Writing migration: {migration_path}")
    
    array_length = max_char_id - 1001 + 1  # Total positions from 1001 to max_char_id
    
    with open(migration_path, "w", encoding="utf-8") as f:
        f.write(f"-- Migration: Update Affinity Data\n")
        f.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Source: {sqlite_path}\n")
        f.write(f"--\n")
        
        char_ids_str = ','.join(str(c) for c in chars)
        if is_incremental:
            f.write(f"-- Type: INCREMENTAL\n")
            f.write(f"-- New array length: {array_length} (chara 1001-{max_char_id})\n")
            f.write(f"-- New character(s): {new_char_ids}\n")
            f.write(f"-- Character IDs: {char_ids_str}\n")
        else:
            f.write(f"-- Type: FULL INITIALIZATION\n")
            f.write(f"-- Array length: {array_length} (chara 1001-{max_char_id})\n")
            f.write(f"-- Array mapping: chara_id 1001 = array[1], 1040 = array[40], 1061 = array[61], etc.\n")
            f.write(f"-- Missing characters are filled with 0\n")
            f.write(f"-- Character IDs: {char_ids_str}\n")
        
        # ===== UPDATE STATEMENTS (inside transaction) =====
        f.write(f"\nBEGIN;\n\n")
        f.write(f"-- Update all {len(affinity_data)} inheritance combinations\n\n")
        
        count = 0
        for (main, left, right), data in affinity_data.items():
            scores = data['affinity_scores']
            base = data['base_affinity']
            
            array_str = 'ARRAY[' + ','.join(map(str, scores)) + ']::int[]'
            f.write(
                f"UPDATE inheritance SET affinity_scores = {array_str}, "
                f"base_affinity = {base} "
                f"WHERE main_chara_id = {main} AND left_chara_id = {left} AND right_chara_id = {right};\n"
            )
            
            count += 1
            if count % 100 == 0:
                f.write("\n")
        
        f.write(f"\nCOMMIT;\n\n")
        
        # ===== CREATE INDEXES (outside transaction — CONCURRENTLY is not allowed in a tx) =====
        f.write(f"-- Expression indexes for affinity sorting\n")
        f.write(f"-- Note: DROP old indexes first, then CREATE CONCURRENTLY new ones\n\n")
        
        if is_incremental:
            # Only create indexes for new character IDs that actually exist in the data
            for char_id in new_char_ids:
                if char_id in chara_rel:  # Only if character has actual data
                    pg_index = char_id - 1000  # PostgreSQL 1-based
                    f.write(f"DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_total_affinity_{char_id};\n")
                    f.write(
                        f"CREATE INDEX CONCURRENTLY idx_inheritance_total_affinity_{char_id} \n"
                        f"    ON inheritance ((COALESCE(affinity_scores[{pg_index}], 0) + COALESCE(race_affinity, 0)) DESC);\n\n"
                    )
        else:
            # Recreate indexes for all characters that exist in the data
            for char_id in chars:
                pg_index = char_id - 1000
                f.write(f"DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_total_affinity_{char_id};\n")
                f.write(
                    f"CREATE INDEX CONCURRENTLY idx_inheritance_total_affinity_{char_id} \n"
                    f"    ON inheritance ((COALESCE(affinity_scores[{pg_index}], 0) + COALESCE(race_affinity, 0)) DESC);\n\n"
                )
            
            f.write("-- Default affinity index (base_affinity + race_affinity)\n")
            f.write("DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_default_affinity;\n")
            f.write("CREATE INDEX CONCURRENTLY idx_inheritance_default_affinity \n")
            f.write("    ON inheritance ((COALESCE(base_affinity, 0) + COALESCE(race_affinity, 0)) DESC);\n\n")
        
        f.write("-- Verify:\n")
        f.write(f"-- SELECT array_length(affinity_scores, 1) FROM inheritance LIMIT 1;  -- Should be {array_length}\n")

    print(f"✅ Migration created!")
    print(f"\n👉 To apply in production, run: python apply_affinity.py")
    
    if is_incremental:
        print(f"\n📊 Summary: INCREMENTAL")
        print(f"   New: {array_length} positions (1001-{max_char_id})")
        print(f"   Updates: {len(affinity_data)} records")
        new_indexes = len([c for c in new_char_ids if c in chara_rel])
        print(f"   Indexes: {new_indexes} new (char IDs: {new_char_ids})")
    else:
        print(f"\n📊 Summary: FULL INITIALIZATION")
        print(f"   Array positions: {array_length} (1001-{max_char_id})")
        print(f"   Characters with data: {len(chars)}")
        print(f"   Updates: {len(affinity_data)} records")
        print(f"   Indexes: {len(chars)} + 1 default")



        range(0,  len(chars));


if __name__ == "__main__":
    main()
