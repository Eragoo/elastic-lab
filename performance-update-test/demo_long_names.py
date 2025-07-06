#!/usr/bin/env python3
"""
Demo script to show long names functionality
"""

from opensearchpy import OpenSearch
import json
import random

def create_opensearch_client():
    """Create OpenSearch client"""
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )
    return client

def demo_long_names():
    """Demo long names functionality"""
    print("Long Names Demo for OpenSearch")
    print("=" * 40)
    
    try:
        client = create_opensearch_client()
        client.info()
        print("✓ Connected to OpenSearch")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return
    
    # Test different search types
    searches = [
        {
            'type': 'Text Search',
            'query': {
                "query": {
                    "match": {
                        "long_name": "Investment Fund"
                    }
                },
                "size": 3
            }
        },
        {
            'type': 'Combined Search',
            'query': {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"long_name": "Technology"}},
                            {"range": {"price": {"gte": 100, "lte": 500}}}
                        ]
                    }
                },
                "size": 3
            }
        },
        {
            'type': 'Long Name Length Filter',
            'query': {
                "query": {
                    "script": {
                        "script": {
                            "source": "doc['long_name.keyword'].value.length() > 150"
                        }
                    }
                },
                "size": 3
            }
        }
    ]
    
    for search in searches:
        print(f"\n{search['type']}:")
        print("-" * 20)
        
        try:
            response = client.search(
                index="instruments",
                body=search['query']
            )
            
            if response['hits']['total']['value'] > 0:
                print(f"Found {response['hits']['total']['value']} results")
                
                for i, hit in enumerate(response['hits']['hits'], 1):
                    source = hit['_source']
                    print(f"\n{i}. ISIN: {source['isin']}")
                    print(f"   Name: {source['name']}")
                    print(f"   Long Name: {source.get('long_name', 'N/A')}")
                    print(f"   Price: ${source['price']:.2f}")
                    print(f"   Long Name Length: {len(source.get('long_name', ''))}")
            else:
                print("No results found")
                
        except Exception as e:
            print(f"Error: {e}")
    
    # Show statistics
    print(f"\n\nStatistics:")
    print("-" * 20)
    
    try:
        # Count documents
        count_response = client.count(index="instruments")
        total_docs = count_response['count']
        print(f"Total documents: {total_docs}")
        
        # Get long name length statistics
        agg_response = client.search(
            index="instruments",
            body={
                "query": {"match_all": {}},
                "aggs": {
                    "long_name_stats": {
                        "stats": {
                            "script": {
                                "source": "doc['long_name.keyword'].value.length()"
                            }
                        }
                    }
                },
                "size": 0
            }
        )
        
        stats = agg_response['aggregations']['long_name_stats']
        print(f"Long Name Length Statistics:")
        print(f"  Average: {stats['avg']:.1f} characters")
        print(f"  Min: {stats['min']:.0f} characters")
        print(f"  Max: {stats['max']:.0f} characters")
        
    except Exception as e:
        print(f"Error getting statistics: {e}")

if __name__ == "__main__":
    demo_long_names() 