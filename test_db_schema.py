#!/usr/bin/env python3
"""
Test script to check database schema and available tables
"""

import os
from dotenv import load_dotenv
from database.cockroach_connector import CockroachConnector

# Load environment variables
load_dotenv()

def main():
    print("🔍 Testing CockroachDB Schema...")
    
    try:
        # Create connector
        connector = CockroachConnector()
        print(f"✅ Connected to {connector.get_provider_name()}")
        
        # Test connection
        if connector.test_connection():
            print("✅ Connection test passed")
        else:
            print("❌ Connection test failed")
            return
        
        # Check available tables
        print("\n📋 Checking available tables...")
        tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """
        
        tables = connector.execute_query(tables_query)
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f"  - {table['table_name']}")
        else:
            print("No tables found in public schema")
        
        # Check for TPC-C specific tables
        print("\n🏪 Checking for TPC-C tables...")
        tpcc_tables = ['warehouse', 'district', 'customer', 'order', 'order_line', 'item', 'stock', 'new_order', 'history']
        
        for table_name in tpcc_tables:
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                result = connector.execute_query(count_query)
                count = result[0]['count'] if result else 0
                print(f"  ✅ {table_name}: {count} rows")
            except Exception as e:
                print(f"  ❌ {table_name}: {str(e)}")
        
        # Close connection
        connector.close_connection()
        print("\n✅ Database schema check completed")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
