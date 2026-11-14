"""
PostgreSQL Schema & Connection Verification Script
"""

import os
import psycopg2

def verify_database(host='localhost', port=6432, dbname='happiness_db', user='postgres', password='postgres'):
    print(f"Connecting to PostgreSQL at {host}:{port}/{dbname}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cur = conn.cursor()
        
        # Check tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]
        print(f"Found {len(tables)} tables in public schema:")
        for t in tables:
            print(f"  [x] {t}")
            
        # Check Foreign Keys
        cur.execute("""
            SELECT conname, conrelid::regclass, confrelid::regclass
            FROM pg_constraint
            WHERE contype = 'f' AND connamespace = 'public'::regnamespace;
        """)
        fks = cur.fetchall()
        print(f"\nForeign Key Constraints ({len(fks)} found):")
        for fk in fks:
            print(f"  [FK] {fk[0]}: {fk[1]} -> {fk[2]}")
            
        # Check Columns of fact_predictions
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'fact_predictions' 
            ORDER BY ordinal_position;
        """)
        cols = cur.fetchall()
        print(f"\nfact_predictions Columns ({len(cols)} columns):")
        for c in cols:
            print(f"  - {c[0]} ({c[1]})")

        cur.close()
        conn.close()
        print("\nDatabase verification SUCCESSFUL!")
        return True
    except Exception as e:
        print(f"Database verification failed: {e}")
        return False

if __name__ == '__main__':
    verify_database()
