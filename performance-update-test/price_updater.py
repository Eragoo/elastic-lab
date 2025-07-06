#!/usr/bin/env python3
"""
Price Updater Script - Continuously updates instrument prices
Logs performance metrics to CSV for real-time analysis
"""

import pandas as pd
import random
import time
import csv
from datetime import datetime
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import sys
import signal

# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    global running
    print(f"\nReceived signal {signum}. Shutting down gracefully...")
    running = False

def create_opensearch_client():
    """Create OpenSearch client"""
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_auth=None,
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )
    return client

def get_all_instruments(client, index_name="instruments"):
    """Get all instrument ISINs from the index"""
    print("Fetching all instrument ISINs...")
    
    try:
        # Use scroll API to get all documents
        response = client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "_source": ["isin"],
                "size": 10000  # Get 10k at a time
            },
            scroll="5m"
        )
        
        isins = []
        scroll_id = response.get('_scroll_id')
        
        # Get first batch
        for hit in response['hits']['hits']:
            isins.append(hit['_source']['isin'])
        
        # Get remaining batches
        while len(response['hits']['hits']) > 0:
            response = client.scroll(
                scroll_id=scroll_id,
                scroll="5m"
            )
            
            for hit in response['hits']['hits']:
                isins.append(hit['_source']['isin'])
        
        # Clean up scroll
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)
        
        print(f"✓ Found {len(isins)} instruments")
        return isins
        
    except Exception as e:
        print(f"✗ Error fetching instruments: {e}")
        return []

def generate_random_price():
    """Generate a random price similar to the original generation logic"""
    price_ranges = [
        (1.0, 10.0),      # Penny stocks
        (10.0, 50.0),     # Small cap
        (50.0, 200.0),    # Mid cap
        (200.0, 1000.0),  # Large cap
        (1000.0, 5000.0)  # High-value stocks
    ]
    
    price_range = random.choice(price_ranges)
    price = round(random.uniform(price_range[0], price_range[1]), 2)
    return price

def prepare_price_updates(isins, iteration_number):
    """Prepare bulk update actions for price changes"""
    actions = []
    
    for isin in isins:
        new_price = generate_random_price()
        
        action = {
            "_op_type": "update",
            "_index": "instruments",
            "_id": isin,
            "_source": {
                "doc": {
                    "price": new_price,
                    "updated_at": datetime.now().isoformat(),
                    "update_iteration": iteration_number
                }
            }
        }
        actions.append(action)
    
    return actions

def bulk_update_prices(client, actions, batch_size=1000):
    """Perform bulk price updates"""
    start_time = time.time()
    
    try:
        success_count = 0
        error_count = 0
        
        # Process in batches
        total_batches = (len(actions) + batch_size - 1) // batch_size
        
        for i in range(0, len(actions), batch_size):
            if not running:  # Check for graceful shutdown
                break
                
            batch = actions[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"  Updating batch {batch_num}/{total_batches} "
                  f"(records {i+1}-{min(i+batch_size, len(actions))})")
            
            # Execute bulk operation
            response = bulk(client, batch)
            
            success_count += response[0]
            if response[1]:
                error_count += len(response[1])
        
        end_time = time.time()
        duration = end_time - start_time
        
        return success_count, error_count, duration
        
    except Exception as e:
        print(f"✗ Error during bulk update: {e}")
        return 0, len(actions), time.time() - start_time

def log_performance_metrics(iteration, total_instruments, success_count, error_count, duration, csv_file="price_update_metrics.csv"):
    """Log performance metrics to CSV"""
    
    # Check if file exists to write header
    file_exists = False
    try:
        with open(csv_file, 'r'):
            file_exists = True
    except FileNotFoundError:
        pass
    
    # Write metrics
    with open(csv_file, 'a', newline='') as f:
        writer = csv.writer(f)
        
        if not file_exists:
            # Write header
            writer.writerow([
                'timestamp', 'iteration', 'total_instruments', 'success_count', 
                'error_count', 'duration_seconds', 'updates_per_second'
            ])
        
        # Write data
        updates_per_second = success_count / duration if duration > 0 else 0
        writer.writerow([
            datetime.now().isoformat(),
            iteration,
            total_instruments,
            success_count,
            error_count,
            round(duration, 2),
            round(updates_per_second, 2)
        ])

def main():
    """Main function to run continuous price updates"""
    global running
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Price Updater - Continuous Instrument Price Updates")
    print("=" * 60)
    print("Press Ctrl+C to stop gracefully")
    print()
    
    # Create OpenSearch client
    try:
        client = create_opensearch_client()
        client.info()  # Test connection
        print("✓ Connected to OpenSearch")
    except Exception as e:
        print(f"✗ Failed to connect to OpenSearch: {e}")
        return
    
    # Get all instruments
    isins = get_all_instruments(client)
    if not isins:
        print("✗ No instruments found. Please run the import script first.")
        return
    
    print(f"✓ Will update {len(isins)} instruments continuously")
    print()
    
    # Initialize CSV file
    csv_file = "price_update_metrics.csv"
    print(f"✓ Logging metrics to: {csv_file}")
    print()
    
    # Continuous update loop
    iteration = 1
    
    while running:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting iteration {iteration}")
            
            # Prepare updates
            actions = prepare_price_updates(isins, iteration)
            
            # Perform bulk update
            success_count, error_count, duration = bulk_update_prices(client, actions)
            
            # Log metrics
            log_performance_metrics(iteration, len(isins), success_count, error_count, duration, csv_file)
            
            # Print summary
            updates_per_second = success_count / duration if duration > 0 else 0
            print(f"  ✓ Iteration {iteration} completed:")
            print(f"    - Updated: {success_count} instruments")
            print(f"    - Errors: {error_count}")
            print(f"    - Duration: {duration:.2f}s")
            print(f"    - Rate: {updates_per_second:.2f} updates/sec")
            print()
            
            iteration += 1
            
            # Short pause between iterations to avoid overwhelming the system
            if running:
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\nReceived interrupt signal. Shutting down...")
            running = False
        except Exception as e:
            print(f"✗ Error in iteration {iteration}: {e}")
            print("Waiting 5 seconds before retry...")
            time.sleep(5)
    
    print(f"\n🏁 Price updater stopped after {iteration-1} iterations")
    print(f"📊 Metrics saved to: {csv_file}")

if __name__ == "__main__":
    main() 