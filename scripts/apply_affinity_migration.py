#!/usr/bin/env python3
"""
Apply affinity_migration.sql using DATABASE_URL from .env file
Works on both Linux and Windows
Usage: python scripts/apply_affinity_migration.py
"""

import os
import sys
import re
import subprocess
from pathlib import Path

def load_env():
    """Load DATABASE_URL from .env file"""
    env_path = Path('.env')
    if not env_path.exists():
        print("❌ Error: .env file not found in current directory")
        sys.exit(1)
    
    database_url = None
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('DATABASE_URL'):
                match = re.match(r'DATABASE_URL\s*=\s*["\']?(.+?)["\']?\s*$', line)
                if match:
                    database_url = match.group(1)
                    break
    
    if not database_url:
        print("❌ Error: DATABASE_URL not found in .env file")
        sys.exit(1)
    
    return database_url

def parse_database_url(url):
    """Parse PostgreSQL connection string"""
    # Format: postgresql://user:password@host:port/database
    pattern = r'^postgresql://([^:]+):([^@]+)@([^:/]+):(\d+)/(.+)$'
    match = re.match(pattern, url)
    
    if not match:
        print("❌ Error: Invalid DATABASE_URL format")
        print("Expected format: postgresql://user:password@host:port/database")
        sys.exit(1)
    
    return {
        'user': match.group(1),
        'password': match.group(2),
        'host': match.group(3),
        'port': match.group(4),
        'database': match.group(5)
    }

def main():
    print("🔍 Reading .env file...")
    
    # Check if migration file exists
    migration_file = Path('affinity_migration.sql')
    if not migration_file.exists():
        print("❌ Error: affinity_migration.sql not found in current directory")
        sys.exit(1)
    
    # Load and parse DATABASE_URL
    database_url = load_env()
    db_config = parse_database_url(database_url)
    
    print("✅ Found DATABASE_URL in .env")
    print(f"📊 Database: {db_config['database']}")
    print(f"🖥️  Host: {db_config['host']}:{db_config['port']}")
    print(f"👤 User: {db_config['user']}")
    print()
    
    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env['PGPASSWORD'] = db_config['password']
    
    print("🚀 Applying affinity_migration.sql...")
    
    try:
        # Execute the SQL file using psql with performance flags
        # -q: quiet mode (suppress notices)
        # -o /dev/null (Linux) or -o NUL (Windows): don't output results
        # --set ON_ERROR_STOP=on: stop on first error
        output_null = 'NUL' if sys.platform == 'win32' else '/dev/null'
        
        result = subprocess.run(
            [
                'psql',
                '-h', db_config['host'],
                '-p', db_config['port'],
                '-U', db_config['user'],
                '-d', db_config['database'],
                '-q',  # Quiet mode
                '--set', 'ON_ERROR_STOP=on',  # Stop on error
                '-f', 'affinity_migration.sql'
            ],
            env=env,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ Migration applied successfully!")
            print()
            print("📝 Output:")
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)
            
            # Verify array length
            print()
            print("🔍 Verifying affinity_scores array length...")
            verify_result = subprocess.run(
                [
                    'psql',
                    '-h', db_config['host'],
                    '-p', db_config['port'],
                    '-U', db_config['user'],
                    '-d', db_config['database'],
                    '-t',
                    '-c', 'SELECT MAX(array_length(affinity_scores, 1)) as max_length FROM inheritance;'
                ],
                env=env,
                capture_output=True,
                text=True
            )
            
            if verify_result.returncode == 0 and verify_result.stdout.strip():
                print(f"✅ Array length: {verify_result.stdout.strip()}")
            
            print()
            print("✨ Done!")
        else:
            print("❌ Migration failed with exit code:", result.returncode)
            if result.stdout:
                print("stdout:", result.stdout)
            if result.stderr:
                print("stderr:", result.stderr)
            sys.exit(1)
            
    except FileNotFoundError:
        print("❌ Error: psql command not found")
        print("Please install PostgreSQL client tools")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error executing migration: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
