#!/usr/bin/env python3
"""
Performance Test Runner - Launch both scripts simultaneously
"""

import subprocess
import sys
import time
from datetime import datetime

def main():
    print("Real-time OpenSearch Performance Test")
    print("=" * 40)
    print()
    print("This script will help you run both performance monitoring scripts:")
    print("1. price_updater.py - Continuously updates all 50k instrument prices")
    print("2. search_performance.py - Continuously performs price range searches")
    print()
    print("Both scripts will log metrics to separate CSV files:")
    print("- price_update_metrics.csv - Update performance data")
    print("- search_performance_metrics.csv - Search performance data")
    print()
    print("Instructions:")
    print("1. Open two terminal windows")
    print("2. In terminal 1, run: python price_updater.py")
    print("3. In terminal 2, run: python search_performance.py")
    print("4. Let both run for several minutes to collect data")
    print("5. Use Ctrl+C to stop each script gracefully")
    print()
    print("Analysis:")
    print("- Compare search times during update operations")
    print("- Look for performance degradation patterns")
    print("- Analyze different price range search types")
    print()
    
    choice = input("Do you want to run a quick test? (y/n): ").lower()
    
    if choice == 'y':
        print("\n" + "=" * 40)
        print("Running quick test...")
        print("This will run the search script for 30 seconds")
        print("while the price updater runs one iteration")
        print()
        
        try:
            # Start search performance script
            print("Starting search performance monitoring...")
            search_proc = subprocess.Popen([sys.executable, "search_performance.py"])
            
            # Wait 5 seconds for search to start
            time.sleep(5)
            
            # Run one iteration of price updater
            print("Running price update iteration...")
            update_proc = subprocess.Popen([sys.executable, "price_updater.py"])
            
            # Let it run for 30 seconds
            print("Collecting data for 30 seconds...")
            time.sleep(30)
            
            # Stop both processes
            print("Stopping processes...")
            search_proc.terminate()
            update_proc.terminate()
            
            # Wait for processes to finish
            search_proc.wait()
            update_proc.wait()
            
            print("✓ Quick test completed!")
            print("Check the CSV files for results:")
            print("- price_update_metrics.csv")
            print("- search_performance_metrics.csv")
            
        except KeyboardInterrupt:
            print("\nTest interrupted by user")
            if 'search_proc' in locals():
                search_proc.terminate()
            if 'update_proc' in locals():
                update_proc.terminate()
        except Exception as e:
            print(f"Error running test: {e}")
    
    else:
        print("\nTo run the full test manually:")
        print("Terminal 1: python price_updater.py")
        print("Terminal 2: python search_performance.py")
        print()
        print("Happy testing! 🚀")

if __name__ == "__main__":
    main() 