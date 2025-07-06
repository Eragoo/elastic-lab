#!/usr/bin/env python3
"""
Search Performance Script - Continuously performs price range searches
Logs performance metrics to CSV for real-time analysis
"""

import random
import time
import csv
from datetime import datetime
from opensearchpy import OpenSearch
import signal
import sys

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

def get_price_statistics(client, index_name="instruments"):
    """Get price statistics to generate meaningful search ranges"""
    try:
        response = client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "aggs": {
                    "price_stats": {
                        "stats": {
                            "field": "price"
                        }
                    }
                },
                "size": 0
            }
        )
        
        stats = response['aggregations']['price_stats']
        return {
            'min': stats['min'],
            'max': stats['max'],
            'avg': stats['avg']
        }
        
    except Exception as e:
        print(f"✗ Error getting price statistics: {e}")
        # Return default ranges if stats fail
        return {'min': 1.0, 'max': 5000.0, 'avg': 250.0}

def generate_price_range(price_stats):
    """Generate various types of price ranges for testing"""
    min_price = price_stats['min']
    max_price = price_stats['max']
    avg_price = price_stats['avg']
    
    # Different types of price ranges to test various scenarios
    range_types = [
        # Narrow ranges (should be fast)
        ('narrow_low', min_price, min_price + 50),
        ('narrow_mid', avg_price - 25, avg_price + 25),
        ('narrow_high', max_price - 100, max_price),
        
        # Medium ranges
        ('medium_low', min_price, avg_price * 0.5),
        ('medium_mid', avg_price * 0.5, avg_price * 1.5),
        ('medium_high', avg_price * 1.5, max_price),
        
        # Wide ranges (should be slower)
        ('wide_all', min_price, max_price),
        ('wide_lower_half', min_price, avg_price),
        ('wide_upper_half', avg_price, max_price),
        
        # Specific ranges
        ('penny_stocks', 1.0, 10.0),
        ('small_cap', 10.0, 50.0),
        ('mid_cap', 50.0, 200.0),
        ('large_cap', 200.0, 1000.0),
        ('high_value', 1000.0, 5000.0)
    ]
    
    # Pick a random range type
    range_type, range_min, range_max = random.choice(range_types)
    
    # Add some randomness to the exact bounds
    range_min = max(min_price, range_min + random.uniform(-10, 10))
    range_max = min(max_price, range_max + random.uniform(-10, 10))
    
    # Ensure min < max
    if range_min >= range_max:
        range_min = range_max - 1
    
    return range_type, round(range_min, 2), round(range_max, 2)

def perform_price_search(client, min_price, max_price, index_name="instruments"):
    """Perform a price range search and measure performance"""
    start_time = time.time()
    
    try:
        # Perform the search
        response = client.search(
            index=index_name,
            body={
                "query": {
                    "range": {
                        "price": {
                            "gte": min_price,
                            "lte": max_price
                        }
                    }
                },
                "size": 100,  # Limit results for consistent timing
                "sort": [{"price": {"order": "asc"}}]
            }
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Extract results
        total_hits = response['hits']['total']['value']
        returned_hits = len(response['hits']['hits'])
        
        # Get some sample prices for verification
        sample_prices = [hit['_source']['price'] for hit in response['hits']['hits'][:5]]
        
        return {
            'success': True,
            'duration': duration,
            'total_hits': total_hits,
            'returned_hits': returned_hits,
            'sample_prices': sample_prices,
            'error': None
        }
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        return {
            'success': False,
            'duration': duration,
            'total_hits': 0,
            'returned_hits': 0,
            'sample_prices': [],
            'error': str(e)
        }

def log_search_metrics(search_id, range_type, min_price, max_price, result, csv_file="search_performance_metrics.csv"):
    """Log search performance metrics to CSV"""
    
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
                'timestamp', 'search_id', 'range_type', 'min_price', 'max_price', 
                'price_range_width', 'success', 'duration_ms', 'total_hits', 
                'returned_hits', 'hits_per_ms', 'sample_prices', 'error'
            ])
        
        # Calculate metrics
        price_range_width = max_price - min_price
        duration_ms = result['duration'] * 1000  # Convert to milliseconds
        hits_per_ms = result['total_hits'] / duration_ms if duration_ms > 0 else 0
        
        # Write data
        writer.writerow([
            datetime.now().isoformat(),
            search_id,
            range_type,
            min_price,
            max_price,
            round(price_range_width, 2),
            result['success'],
            round(duration_ms, 2),
            result['total_hits'],
            result['returned_hits'],
            round(hits_per_ms, 2),
            str(result['sample_prices']),
            result['error']
        ])

def main():
    """Main function to run continuous search performance testing"""
    global running
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Search Performance Tester - Continuous Price Range Searches")
    print("=" * 65)
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
    
    # Get price statistics
    print("Getting price statistics...")
    price_stats = get_price_statistics(client)
    print(f"✓ Price range: ${price_stats['min']:.2f} - ${price_stats['max']:.2f} (avg: ${price_stats['avg']:.2f})")
    print()
    
    # Initialize CSV file
    csv_file = "search_performance_metrics.csv"
    print(f"✓ Logging metrics to: {csv_file}")
    print()
    
    # Continuous search loop
    search_id = 1
    
    while running:
        try:
            # Generate random price range
            range_type, min_price, max_price = generate_price_range(price_stats)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Search #{search_id} - "
                  f"{range_type}: ${min_price:.2f} - ${max_price:.2f}")
            
            # Perform search
            result = perform_price_search(client, min_price, max_price)
            
            # Log metrics
            log_search_metrics(search_id, range_type, min_price, max_price, result, csv_file)
            
            # Print summary
            if result['success']:
                print(f"  ✓ Found {result['total_hits']} instruments in {result['duration']*1000:.2f}ms")
                if result['sample_prices']:
                    print(f"    Sample prices: {result['sample_prices']}")
            else:
                print(f"  ✗ Search failed: {result['error']}")
            
            print()
            search_id += 1
            
            # Short pause between searches
            if running:
                time.sleep(1)  # 1 second between searches
                
        except KeyboardInterrupt:
            print("\nReceived interrupt signal. Shutting down...")
            running = False
        except Exception as e:
            print(f"✗ Error in search #{search_id}: {e}")
            print("Waiting 2 seconds before retry...")
            time.sleep(2)
    
    print(f"\n🏁 Search performance tester stopped after {search_id-1} searches")
    print(f"📊 Metrics saved to: {csv_file}")

if __name__ == "__main__":
    main() 