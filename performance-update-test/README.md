# OpenSearch Performance Testing Suite

Real-time performance testing for OpenSearch with instrument data, featuring price updates and various search types including long name text search.

## Quick Start

### 1. Start OpenSearch
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Data
```bash
# Create index with long_name field (100-200 characters)
python create_index.py

# Generate 50,000 test instruments
python generate_test_data.py

# Import data into OpenSearch
python import_instruments.py
```

### 4. Run Performance Tests

**Terminal 1 - Price Updater:**
```bash
python price_updater.py
```

**Terminal 2 - Search Performance Tester:**
```bash
python search_performance.py
```

Let both run for 5-10 minutes, then stop with `Ctrl+C`.

### 5. Analyze Results
```bash
python analyze_performance.py
```

## What It Tests

### Search Types:
- **Price Range Search**: `$1-60`, `$125-375`, `$1-5000`
- **Text Search**: "Investment", "Technology", "Fund" in long names
- **Combined Search**: Text + Price range

### Performance Metrics:
- Search duration during price updates vs. normal operation
- Different search type performance comparison
- Impact of bulk updates on search latency

## Demo & Exploration

```bash
# Test long name functionality
python demo_long_names.py

# Quick automated test
python run_performance_test.py
```

## Output Files

- `price_update_metrics.csv` - Update performance data
- `search_performance_metrics.csv` - Search performance data
- `performance_analysis.png` - Generated charts

## OpenSearch Dashboards

Access at: http://localhost:5601
Create index pattern: `instruments*`

## Data Structure

Each instrument contains:
- `isin` - Unique identifier (12 chars)
- `name` - Short name (20-50 chars)
- `long_name` - Extended name (100-200 chars)
- `price` - Decimal price ($1-$5000)

## Expected Results

- Text searches are slower than price range searches
- Combined searches have highest latency
- 20-50% performance degradation during bulk updates
- Quick recovery after updates complete

---

**Note**: Ensure Docker is running and ports 9200, 5601 are available. 