#!/usr/bin/env python3
"""
Script to parse CSV and upsert instrument data into OpenSearch
"""

import pandas as pd
import json
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import time
from datetime import datetime

def create_opensearch_client():
    """Create OpenSearch client"""
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_auth=None,  # No authentication since we disabled security
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )
    return client

def test_connection():
    """Test connection to OpenSearch"""
    try:
        client = create_opensearch_client()
        info = client.info()
        print("✓ Connected to OpenSearch successfully!")
        return True
    except Exception as e:
        print(f"✗ Failed to connect to OpenSearch: {e}")
        return False

def check_index_exists(client, index_name="instruments"):
    """Check if the instruments index exists"""
    try:
        exists = client.indices.exists(index=index_name)
        if exists:
            print(f"✓ Index '{index_name}' exists")
            return True
        else:
            print(f"✗ Index '{index_name}' does not exist")
            print("Please run 'python create_index.py' first")
            return False
    except Exception as e:
        print(f"✗ Error checking index: {e}")
        return False

def read_csv_data(filename="instruments_test_data.csv"):
    """Read instrument data from CSV file"""
    try:
        print(f"Reading data from '{filename}'...")
        df = pd.read_csv(filename)
        
        # Validate required columns
        required_columns = ['isin', 'name', 'long_name', 'price']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"✗ Missing required columns: {missing_columns}")
            return None
        
        # Basic data validation
        print(f"✓ Loaded {len(df)} records from CSV")
        print(f"✓ Columns: {list(df.columns)}")
        
        # Check for duplicates
        duplicates = df.duplicated(subset=['isin']).sum()
        if duplicates > 0:
            print(f"⚠ Warning: Found {duplicates} duplicate ISINs, removing duplicates...")
            df = df.drop_duplicates(subset=['isin'], keep='first')
            print(f"✓ After removing duplicates: {len(df)} records")
        
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.any():
            print(f"⚠ Warning: Missing values found:")
            for col, count in missing_values.items():
                if count > 0:
                    print(f"  - {col}: {count} missing values")
            
            # Remove rows with missing critical data
            df = df.dropna(subset=['isin', 'name', 'long_name', 'price'])
            print(f"✓ After removing rows with missing data: {len(df)} records")
        
        return df
        
    except FileNotFoundError:
        print(f"✗ File '{filename}' not found")
        print("Please run 'python generate_test_data.py' first")
        return None
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return None

def prepare_bulk_data(df, index_name="instruments"):
    """Prepare data for bulk operations"""
    print("Preparing bulk data...")
    
    actions = []
    for _, row in df.iterrows():
        # Create the action for bulk API
        action = {
            "_op_type": "index",  # This will upsert (create or update)
            "_index": index_name,
            "_id": row['isin'],  # Use ISIN as document ID
            "_source": {
                "isin": row['isin'],
                "name": row['name'],
                "long_name": row['long_name'],
                "price": float(row['price']),
                "updated_at": datetime.now().isoformat()
            }
        }
        actions.append(action)
    
    print(f"✓ Prepared {len(actions)} actions for bulk operation")
    return actions

def bulk_upsert_data(client, actions, batch_size=1000):
    """Perform bulk upsert operation"""
    print(f"Starting bulk upsert with batch size: {batch_size}")
    start_time = time.time()
    
    try:
        # Perform bulk operation
        success_count = 0
        error_count = 0
        
        # Process in batches
        for i in range(0, len(actions), batch_size):
            batch = actions[i:i+batch_size]
            
            print(f"Processing batch {i//batch_size + 1}/{(len(actions) + batch_size - 1)//batch_size} "
                  f"(records {i+1}-{min(i+batch_size, len(actions))})")
            
            # Execute bulk operation
            response = bulk(client, batch)
            
            # Count successes and errors
            success_count += response[0]
            if response[1]:  # If there are errors
                error_count += len(response[1])
                print(f"⚠ Batch had {len(response[1])} errors")
                
                # Print first few errors for debugging
                for error in response[1][:3]:  # Show first 3 errors
                    print(f"  Error: {error}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n" + "="*50)
        print(f"Bulk upsert completed!")
        print(f"✓ Successfully processed: {success_count} records")
        if error_count > 0:
            print(f"✗ Errors: {error_count} records")
        print(f"⏱ Time taken: {duration:.2f} seconds")
        print(f"📊 Rate: {success_count/duration:.2f} records/second")
        
        return success_count, error_count
        
    except Exception as e:
        print(f"✗ Error during bulk upsert: {e}")
        return 0, len(actions)

def verify_data(client, index_name="instruments", sample_size=5):
    """Verify that data was inserted correctly"""
    print(f"\nVerifying data in index '{index_name}'...")
    
    try:
        # Get index statistics
        stats = client.cat.count(index=index_name, format='json')
        total_docs = int(stats[0]['count'])
        print(f"✓ Total documents in index: {total_docs}")
        
        # Get a few sample documents
        search_response = client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "size": sample_size
            }
        )
        
        print(f"\nSample documents:")
        for i, hit in enumerate(search_response['hits']['hits'], 1):
            source = hit['_source']
            long_name_preview = source.get('long_name', 'N/A')[:50] + "..." if len(source.get('long_name', '')) > 50 else source.get('long_name', 'N/A')
            print(f"{i}. ISIN: {source['isin']}, Name: {source['name']}")
            print(f"   Long Name: {long_name_preview}")
            print(f"   Price: ${source['price']:.2f}")
        
        return total_docs
        
    except Exception as e:
        print(f"✗ Error verifying data: {e}")
        return 0

def main():
    """Main function to orchestrate the import process"""
    print("Step 3: Import instruments data into OpenSearch")
    print("=" * 50)
    
    # Test connection
    if not test_connection():
        return
    
    # Create client
    client = create_opensearch_client()
    
    # Check if index exists
    if not check_index_exists(client):
        return
    
    # Read CSV data
    df = read_csv_data()
    if df is None:
        return
    
    # Prepare bulk data
    actions = prepare_bulk_data(df)
    
    # Perform bulk upsert
    success_count, error_count = bulk_upsert_data(client, actions)
    
    if success_count > 0:
        # Verify data
        verify_data(client)
        
        print(f"\n🎉 Import completed successfully!")
        print(f"You can now view your data at: http://localhost:5601")
    else:
        print(f"\n❌ Import failed - no records were successfully inserted")

if __name__ == "__main__":
    main() 