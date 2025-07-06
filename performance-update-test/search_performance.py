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

def generate_search_query(price_stats):
    """Generate various types of search queries for testing"""
    min_price = price_stats['min']
    max_price = price_stats['max']
    avg_price = price_stats['avg']
    
    # Different types of search queries to test various scenarios
    search_types = [
        # Price range searches
        ('price_narrow_low', 'price_range', min_price, min_price + 50),
        ('price_narrow_mid', 'price_range', avg_price - 25, avg_price + 25),
        ('price_narrow_high', 'price_range', max_price - 100, max_price),
        ('price_medium_low', 'price_range', min_price, avg_price * 0.5),
        ('price_medium_mid', 'price_range', avg_price * 0.5, avg_price * 1.5),
        ('price_medium_high', 'price_range', avg_price * 1.5, max_price),
        ('price_wide_all', 'price_range', min_price, max_price),
        ('price_wide_lower_half', 'price_range', min_price, avg_price),
        ('price_wide_upper_half', 'price_range', avg_price, max_price),
        
        # Long name text searches
        ('long_name_investment', 'text_search', 'Investment'),
        ('long_name_fund', 'text_search', 'Fund'),
        ('long_name_technology', 'text_search', 'Technology'),
        ('long_name_global', 'text_search', 'Global'),
        ('long_name_strategy', 'text_search', 'Strategy'),
        ('long_name_financial', 'text_search', 'Financial'),
        ('long_name_management', 'text_search', 'Management'),
        ('long_name_portfolio', 'text_search', 'Portfolio'),
        ('long_name_diversified', 'text_search', 'Diversified'),
        ('long_name_sustainable', 'text_search', 'Sustainable'),
        
        # Combined searches (price + text)
        ('combined_tech_mid_price', 'combined', 'Technology', avg_price * 0.5, avg_price * 1.5),
        ('combined_global_high_price', 'combined', 'Global', avg_price * 1.5, max_price),
        ('combined_fund_low_price', 'combined', 'Fund', min_price, avg_price * 0.5),
    ]
    
    # Pick a random search type
    search_info = random.choice(search_types)
    search_type = search_info[0]
    query_type = search_info[1]
    
    if query_type == 'price_range':
        range_min = max(min_price, search_info[2] + random.uniform(-10, 10))
        range_max = min(max_price, search_info[3] + random.uniform(-10, 10))
        if range_min >= range_max:
            range_min = range_max - 1
        return search_type, query_type, round(range_min, 2), round(range_max, 2), None
    elif query_type == 'text_search':
        return search_type, query_type, None, None, search_info[2]
    elif query_type == 'combined':
        range_min = max(min_price, search_info[3] + random.uniform(-10, 10))
        range_max = min(max_price, search_info[4] + random.uniform(-10, 10))
        if range_min >= range_max:
            range_min = range_max - 1
        return search_type, query_type, round(range_min, 2), round(range_max, 2), search_info[2]
    
    return search_type, query_type, None, None, None

def perform_search(client, query_type, min_price=None, max_price=None, text_query=None, index_name="instruments"):
    """Perform different types of searches and measure performance"""
    start_time = time.time()
    
    try:
        # Build query based on type
        if query_type == 'price_range':
            search_body = {
                "query": {
                    "range": {
                        "price": {
                            "gte": min_price,
                            "lte": max_price
                        }
                    }
                },
                "size": 100,
                "sort": [{"price": {"order": "asc"}}]
            }
        elif query_type == 'text_search':
            search_body = {
                "query": {
                    "match": {
                        "long_name": {
                            "query": text_query,
                            "operator": "and"
                        }
                    }
                },
                "size": 100,
                "sort": [{"price": {"order": "asc"}}]
            }
        elif query_type == 'combined':
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "long_name": {
                                        "query": text_query,
                                        "operator": "and"
                                    }
                                }
                            },
                            {
                                "range": {
                                    "price": {
                                        "gte": min_price,
                                        "lte": max_price
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": 100,
                "sort": [{"price": {"order": "asc"}}]
            }
        else:
            raise ValueError(f"Unknown query type: {query_type}")
        
        # Perform the search
        response = client.search(index=index_name, body=search_body)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Extract results
        total_hits = response['hits']['total']['value']
        returned_hits = len(response['hits']['hits'])
        
        # Get some sample data for verification
        sample_data = []
        for hit in response['hits']['hits'][:3]:
            source = hit['_source']
            sample_data.append({
                'isin': source['isin'],
                'price': source['price'],
                'long_name': source.get('long_name', 'N/A')[:50] + '...' if len(source.get('long_name', '')) > 50 else source.get('long_name', 'N/A')
            })
        
        return {
            'success': True,
            'duration': duration,
            'total_hits': total_hits,
            'returned_hits': returned_hits,
            'sample_data': sample_data,
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
            'sample_data': [],
            'error': str(e)
        }

def log_search_metrics(search_id, search_type, query_type, min_price, max_price, text_query, result, csv_file="search_performance_metrics.csv"):
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
                'timestamp', 'search_id', 'search_type', 'query_type', 'min_price', 'max_price', 
                'price_range_width', 'text_query', 'success', 'duration_ms', 'total_hits', 
                'returned_hits', 'hits_per_ms', 'sample_data', 'error'
            ])
        
        # Calculate metrics
        price_range_width = (max_price - min_price) if min_price and max_price else 0
        duration_ms = result['duration'] * 1000  # Convert to milliseconds
        hits_per_ms = result['total_hits'] / duration_ms if duration_ms > 0 else 0
        
        # Write data
        writer.writerow([
            datetime.now().isoformat(),
            search_id,
            search_type,
            query_type,
            min_price if min_price else '',
            max_price if max_price else '',
            round(price_range_width, 2) if price_range_width > 0 else '',
            text_query if text_query else '',
            result['success'],
            round(duration_ms, 2),
            result['total_hits'],
            result['returned_hits'],
            round(hits_per_ms, 2),
            str(result['sample_data']),
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
            # Generate random search query
            search_type, query_type, min_price, max_price, text_query = generate_search_query(price_stats)
            
            # Create search description
            if query_type == 'price_range':
                search_desc = f"{search_type}: ${min_price:.2f} - ${max_price:.2f}"
            elif query_type == 'text_search':
                search_desc = f"{search_type}: '{text_query}'"
            elif query_type == 'combined':
                search_desc = f"{search_type}: '{text_query}' + ${min_price:.2f} - ${max_price:.2f}"
            else:
                search_desc = f"{search_type}: unknown"
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Search #{search_id} - {search_desc}")
            
            # Perform search
            result = perform_search(client, query_type, min_price, max_price, text_query)
            
            # Log metrics
            log_search_metrics(search_id, search_type, query_type, min_price, max_price, text_query, result, csv_file)
            
            # Print summary
            if result['success']:
                print(f"  ✓ Found {result['total_hits']} instruments in {result['duration']*1000:.2f}ms")
                if result['sample_data']:
                    print(f"    Sample results:")
                    for i, sample in enumerate(result['sample_data'][:2], 1):
                        print(f"      {i}. {sample['isin']} - ${sample['price']:.2f}")
                        if query_type != 'price_range':
                            print(f"         {sample['long_name']}")
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