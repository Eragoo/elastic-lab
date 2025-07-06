#!/usr/bin/env python3
"""
Performance Analysis Script - Analyze the impact of updates on search performance
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

def load_data():
    """Load performance data from CSV files"""
    try:
        # Load update metrics
        update_df = pd.read_csv('price_update_metrics.csv')
        update_df['timestamp'] = pd.to_datetime(update_df['timestamp'])
        print(f"✓ Loaded {len(update_df)} update records")
        
        # Load search metrics
        search_df = pd.read_csv('search_performance_metrics.csv')
        search_df['timestamp'] = pd.to_datetime(search_df['timestamp'])
        print(f"✓ Loaded {len(search_df)} search records")
        
        return update_df, search_df
        
    except FileNotFoundError as e:
        print(f"✗ CSV file not found: {e}")
        print("Please run the performance test scripts first")
        return None, None
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        return None, None

def analyze_search_performance(search_df):
    """Analyze search performance metrics"""
    print("\n" + "=" * 50)
    print("SEARCH PERFORMANCE ANALYSIS")
    print("=" * 50)
    
    # Basic statistics
    print(f"Total searches: {len(search_df)}")
    print(f"Successful searches: {search_df['success'].sum()}")
    print(f"Failed searches: {(~search_df['success']).sum()}")
    print(f"Success rate: {search_df['success'].mean()*100:.1f}%")
    print()
    
    # Performance metrics
    successful_searches = search_df[search_df['success']]
    if len(successful_searches) > 0:
        print("Search Duration Statistics:")
        print(f"  Average: {successful_searches['duration_ms'].mean():.2f}ms")
        print(f"  Median: {successful_searches['duration_ms'].median():.2f}ms")
        print(f"  Min: {successful_searches['duration_ms'].min():.2f}ms")
        print(f"  Max: {successful_searches['duration_ms'].max():.2f}ms")
        print(f"  Std Dev: {successful_searches['duration_ms'].std():.2f}ms")
        print()
        
        # Performance by range type
        print("Performance by Range Type:")
        range_stats = successful_searches.groupby('range_type')['duration_ms'].agg(['mean', 'median', 'count'])
        print(range_stats.round(2))
        print()

def analyze_update_performance(update_df):
    """Analyze update performance metrics"""
    print("\n" + "=" * 50)
    print("UPDATE PERFORMANCE ANALYSIS")
    print("=" * 50)
    
    if len(update_df) == 0:
        print("No update data available")
        return
        
    print(f"Total update iterations: {len(update_df)}")
    print(f"Total instruments updated: {update_df['success_count'].sum()}")
    print(f"Total errors: {update_df['error_count'].sum()}")
    print()
    
    print("Update Duration Statistics:")
    print(f"  Average: {update_df['duration_seconds'].mean():.2f}s")
    print(f"  Median: {update_df['duration_seconds'].median():.2f}s")
    print(f"  Min: {update_df['duration_seconds'].min():.2f}s")
    print(f"  Max: {update_df['duration_seconds'].max():.2f}s")
    print()
    
    print("Update Rate Statistics:")
    print(f"  Average: {update_df['updates_per_second'].mean():.2f} updates/sec")
    print(f"  Median: {update_df['updates_per_second'].median():.2f} updates/sec")
    print(f"  Min: {update_df['updates_per_second'].min():.2f} updates/sec")
    print(f"  Max: {update_df['updates_per_second'].max():.2f} updates/sec")
    print()

def correlation_analysis(update_df, search_df):
    """Analyze correlation between updates and search performance"""
    print("\n" + "=" * 50)
    print("CORRELATION ANALYSIS")
    print("=" * 50)
    
    if len(update_df) == 0 or len(search_df) == 0:
        print("Insufficient data for correlation analysis")
        return
    
    # Find search times during update periods
    update_periods = []
    for _, update_row in update_df.iterrows():
        update_start = update_row['timestamp']
        update_end = update_start + pd.Timedelta(seconds=update_row['duration_seconds'])
        
        # Find searches that occurred during this update
        during_update = search_df[
            (search_df['timestamp'] >= update_start) & 
            (search_df['timestamp'] <= update_end) &
            (search_df['success'] == True)
        ]
        
        if len(during_update) > 0:
            update_periods.append({
                'iteration': update_row['iteration'],
                'update_duration': update_row['duration_seconds'],
                'updates_per_second': update_row['updates_per_second'],
                'search_count': len(during_update),
                'avg_search_duration': during_update['duration_ms'].mean(),
                'median_search_duration': during_update['duration_ms'].median(),
                'max_search_duration': during_update['duration_ms'].max()
            })
    
    if update_periods:
        correlation_df = pd.DataFrame(update_periods)
        print(f"Found {len(correlation_df)} update periods with concurrent searches")
        print()
        
        # Calculate correlations
        correlations = {
            'update_duration_vs_search_time': correlation_df['update_duration'].corr(correlation_df['avg_search_duration']),
            'update_rate_vs_search_time': correlation_df['updates_per_second'].corr(correlation_df['avg_search_duration']),
        }
        
        print("Correlation Coefficients:")
        for name, corr in correlations.items():
            print(f"  {name}: {corr:.3f}")
            if abs(corr) > 0.5:
                print(f"    -> {'Strong' if abs(corr) > 0.7 else 'Moderate'} correlation")
            elif abs(corr) > 0.3:
                print(f"    -> Weak correlation")
            else:
                print(f"    -> No significant correlation")
        print()
        
        # Time-based analysis
        search_without_updates = search_df[
            ~search_df['timestamp'].isin(
                pd.concat([
                    pd.date_range(start=update_row['timestamp'], 
                                end=update_row['timestamp'] + pd.Timedelta(seconds=update_row['duration_seconds']), 
                                freq='1s')
                    for _, update_row in update_df.iterrows()
                ])
            ) & (search_df['success'] == True)
        ]
        
        if len(search_without_updates) > 0:
            print("Performance Comparison:")
            print(f"  Search time during updates: {correlation_df['avg_search_duration'].mean():.2f}ms (avg)")
            print(f"  Search time without updates: {search_without_updates['duration_ms'].mean():.2f}ms (avg)")
            
            impact = ((correlation_df['avg_search_duration'].mean() - search_without_updates['duration_ms'].mean()) 
                     / search_without_updates['duration_ms'].mean()) * 100
            print(f"  Performance impact: {impact:+.1f}% during updates")
    else:
        print("No concurrent update/search periods found")

def create_visualizations(update_df, search_df):
    """Create visualizations if matplotlib is available"""
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        print("\n" + "=" * 50)
        print("CREATING VISUALIZATIONS")
        print("=" * 50)
        
        # Set style
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Search duration over time
        if len(search_df) > 0:
            successful_searches = search_df[search_df['success']]
            axes[0, 0].plot(successful_searches['timestamp'], successful_searches['duration_ms'], 'b-', alpha=0.7)
            axes[0, 0].set_title('Search Duration Over Time')
            axes[0, 0].set_ylabel('Duration (ms)')
            axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Update performance over time
        if len(update_df) > 0:
            axes[0, 1].plot(update_df['timestamp'], update_df['updates_per_second'], 'r-', alpha=0.7)
            axes[0, 1].set_title('Update Rate Over Time')
            axes[0, 1].set_ylabel('Updates/Second')
            axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Search duration by range type
        if len(search_df) > 0:
            successful_searches = search_df[search_df['success']]
            if len(successful_searches) > 0:
                axes[1, 0].boxplot([successful_searches[successful_searches['range_type'] == rt]['duration_ms'].values 
                                  for rt in successful_searches['range_type'].unique()], 
                                 labels=successful_searches['range_type'].unique())
                axes[1, 0].set_title('Search Duration by Range Type')
                axes[1, 0].set_ylabel('Duration (ms)')
                axes[1, 0].tick_params(axis='x', rotation=45)
        
        # Combined timeline
        if len(update_df) > 0 and len(search_df) > 0:
            ax_combined = axes[1, 1]
            
            # Plot search times
            successful_searches = search_df[search_df['success']]
            ax_combined.scatter(successful_searches['timestamp'], successful_searches['duration_ms'], 
                              c='blue', alpha=0.5, s=10, label='Search Duration')
            
            # Plot update periods as vertical lines
            for _, update_row in update_df.iterrows():
                ax_combined.axvline(x=update_row['timestamp'], color='red', alpha=0.7, 
                                  linestyle='--', label='Update Start' if _ == 0 else "")
            
            ax_combined.set_title('Search Performance vs Update Timeline')
            ax_combined.set_ylabel('Search Duration (ms)')
            ax_combined.legend()
            ax_combined.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('performance_analysis.png', dpi=300, bbox_inches='tight')
        print("✓ Visualizations saved to 'performance_analysis.png'")
        
        # Show plot
        plt.show()
        
    except ImportError:
        print("matplotlib not available - skipping visualizations")
        print("Install with: pip install matplotlib seaborn")

def main():
    """Main analysis function"""
    print("Performance Analysis - OpenSearch Update Impact")
    print("=" * 60)
    
    # Load data
    update_df, search_df = load_data()
    
    if update_df is None or search_df is None:
        return
    
    # Perform analysis
    analyze_search_performance(search_df)
    analyze_update_performance(update_df)
    correlation_analysis(update_df, search_df)
    create_visualizations(update_df, search_df)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("Analysis complete! Key findings:")
    print("1. Check correlation coefficients for update impact")
    print("2. Compare search times during vs. without updates")
    print("3. Review performance trends over time")
    print("4. Analyze which search types are most affected")
    print()
    print("For continuous monitoring, keep both scripts running longer")
    print("to collect more data points for better statistical analysis.")

if __name__ == "__main__":
    main() 