#!/usr/bin/env python3
"""
Script to create OpenSearch index for Instruments
"""

from opensearchpy import OpenSearch
import json

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

def create_instrument_index():
    """Create the instruments index with proper mapping"""
    client = create_opensearch_client()
    
    # Define the index mapping
    index_mapping = {
        "mappings": {
            "properties": {
                "isin": {
                    "type": "keyword"  # ISIN should be exact match
                },
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "long_name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "price": {
                    "type": "scaled_float",
                    "scaling_factor": 100  # For price precision (2 decimal places)
                }
            }
        }
    }
    
    index_name = "instruments"
    
    try:
        # Check if index exists
        if client.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists. Deleting it first...")
            client.indices.delete(index=index_name)
        
        # Create the index
        response = client.indices.create(
            index=index_name,
            body=index_mapping
        )
        
        print(f"Successfully created index '{index_name}'")
        print(f"Response: {json.dumps(response, indent=2)}")
        
        # Verify index was created
        index_info = client.indices.get(index=index_name)
        print(f"\nIndex info: {json.dumps(index_info, indent=2)}")
        
        return True
        
    except Exception as e:
        print(f"Error creating index: {e}")
        return False

def test_connection():
    """Test connection to OpenSearch"""
    try:
        client = create_opensearch_client()
        info = client.info()
        print("Connected to OpenSearch successfully!")
        print(f"Cluster info: {json.dumps(info, indent=2)}")
        return True
    except Exception as e:
        print(f"Failed to connect to OpenSearch: {e}")
        return False

if __name__ == "__main__":
    print("Step 1: Creating OpenSearch index for Instruments")
    print("=" * 50)
    
    # Test connection first
    if test_connection():
        print("\n" + "=" * 50)
        # Create the index
        create_instrument_index()
    else:
        print("Please make sure OpenSearch is running: docker-compose up -d") 