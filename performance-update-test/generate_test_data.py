#!/usr/bin/env python3
"""
Script to generate random test data for instruments
"""

import pandas as pd
import random
import string
from decimal import Decimal

def generate_isin(country_code="US"):
    """Generate a realistic ISIN code"""
    # ISIN format: 2 letter country code + 9 alphanumeric characters + 1 check digit
    # For simplicity, we'll generate the first 11 characters and use a random check digit
    alphanumeric = string.ascii_uppercase + string.digits
    security_identifier = ''.join(random.choices(alphanumeric, k=9))
    check_digit = random.randint(0, 9)
    return f"{country_code}{security_identifier}{check_digit}"

def generate_instrument_name():
    """Generate realistic instrument names"""
    prefixes = [
        "Global", "International", "Advanced", "Dynamic", "Strategic", "Premier", 
        "Elite", "Innovative", "Sustainable", "Digital", "Smart", "Future",
        "Alpha", "Beta", "Gamma", "Delta", "Omega", "Prime", "Core", "Edge"
    ]
    
    companies = [
        "TechCorp", "DataSystems", "CloudVentures", "BioMedical", "EnergyPlus",
        "FinanceGroup", "ManufacturingCo", "RetailChain", "TransportHub", "MediaWorks",
        "HealthServices", "ConsumerGoods", "IndustrialSolutions", "AgriTech", "RealEstate",
        "Telecommunications", "Automotive", "Aerospace", "Pharmaceuticals", "Utilities"
    ]
    
    suffixes = [
        "Holdings", "Industries", "Solutions", "Technologies", "Systems", "Services",
        "Group", "Corporation", "Enterprises", "Partners", "Ventures", "International",
        "Global", "Limited", "Inc", "LLC", "AG", "SA", "PLC", "GmbH"
    ]
    
    # Generate different types of instrument names
    name_types = [
        f"{random.choice(prefixes)} {random.choice(companies)} {random.choice(suffixes)}",
        f"{random.choice(companies)} {random.choice(suffixes)}",
        f"{random.choice(prefixes)} {random.choice(companies)}",
        f"{random.choice(companies)} {random.choice(prefixes)} {random.choice(suffixes)}"
    ]
    
    return random.choice(name_types)

def generate_long_instrument_name():
    """Generate long realistic instrument names (100-200 characters)"""
    
    # Extended components for longer names
    business_areas = [
        "Financial Services", "Investment Banking", "Asset Management", "Private Equity",
        "Venture Capital", "Real Estate Investment", "Infrastructure Development",
        "Technology Innovation", "Digital Transformation", "Artificial Intelligence",
        "Machine Learning", "Data Analytics", "Cloud Computing", "Cybersecurity",
        "Biotechnology Research", "Pharmaceutical Development", "Medical Devices",
        "Healthcare Solutions", "Renewable Energy", "Solar Power Generation",
        "Wind Energy Systems", "Energy Storage Solutions", "Smart Grid Technology",
        "Transportation Services", "Logistics Management", "Supply Chain Solutions"
    ]
    
    geographic_regions = [
        "North America", "Europe", "Asia-Pacific", "Latin America", "Middle East",
        "Scandinavia", "Eastern Europe", "Southeast Asia", "Sub-Saharan Africa",
        "Western Europe", "Central Asia", "Caribbean", "Pacific Islands",
        "Mediterranean", "Baltic States", "Nordic Region", "Emerging Markets"
    ]
    
    fund_types = [
        "Equity Fund", "Bond Fund", "Hybrid Fund", "Index Fund", "ETF",
        "Mutual Fund", "Hedge Fund", "Private Equity Fund", "Real Estate Fund",
        "Infrastructure Fund", "Commodity Fund", "Currency Fund", "Derivatives Fund",
        "Alternative Investment Fund", "Sustainable Investment Fund", "Impact Fund"
    ]
    
    investment_strategies = [
        "Growth Strategy", "Value Strategy", "Momentum Strategy", "Dividend Strategy",
        "Quality Strategy", "Low Volatility Strategy", "High Yield Strategy",
        "Multi-Factor Strategy", "ESG Strategy", "Quantitative Strategy",
        "Fundamental Analysis Strategy", "Technical Analysis Strategy",
        "Market Neutral Strategy", "Long Short Strategy", "Arbitrage Strategy"
    ]
    
    # Generate different patterns of long names
    patterns = [
        f"{random.choice(business_areas)} {random.choice(geographic_regions)} {random.choice(fund_types)} - {random.choice(investment_strategies)} with Enhanced Risk Management and Diversified Portfolio Allocation",
        
        f"International {random.choice(business_areas)} and {random.choice(business_areas)} {random.choice(fund_types)} focused on {random.choice(geographic_regions)} Markets with Sustainable Investment Approach",
        
        f"{random.choice(geographic_regions)} {random.choice(business_areas)} {random.choice(fund_types)} implementing {random.choice(investment_strategies)} and Advanced Portfolio Optimization Techniques",
        
        f"Global {random.choice(business_areas)} Investment Platform featuring {random.choice(fund_types)} with {random.choice(investment_strategies)} and Multi-Asset Class Diversification",
        
        f"{random.choice(fund_types)} for {random.choice(geographic_regions)} {random.choice(business_areas)} Sector with Focus on {random.choice(investment_strategies)} and Long-Term Value Creation",
        
        f"Diversified {random.choice(business_areas)} and {random.choice(business_areas)} {random.choice(fund_types)} targeting {random.choice(geographic_regions)} with {random.choice(investment_strategies)}",
        
        f"Strategic {random.choice(business_areas)} Investment {random.choice(fund_types)} for {random.choice(geographic_regions)} Markets emphasizing {random.choice(investment_strategies)} and Risk-Adjusted Returns"
    ]
    
    long_name = random.choice(patterns)
    
    # Ensure the name is within 100-200 characters
    if len(long_name) < 100:
        # Add more details to reach minimum length
        additional_details = [
            "with Professional Management", "and Institutional Grade Infrastructure",
            "featuring Advanced Analytics", "and Regulatory Compliance",
            "including ESG Integration", "and Transparent Reporting",
            "with Daily Liquidity", "and Competitive Fee Structure"
        ]
        while len(long_name) < 100:
            long_name += f" {random.choice(additional_details)}"
    
    # Trim if too long
    if len(long_name) > 200:
        long_name = long_name[:197] + "..."
    
    return long_name

def generate_price():
    """Generate realistic stock prices"""
    # Generate prices in different ranges to simulate various types of instruments
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

def create_test_data(num_instruments=50):
    """Create test data for instruments"""
    print(f"Generating {num_instruments} test instruments...")
    
    instruments = []
    used_isins = set()  # To ensure unique ISINs
    
    countries = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "CH", "NL", "SE"]
    
    for i in range(num_instruments):
        # Generate unique ISIN
        while True:
            country = random.choice(countries)
            isin = generate_isin(country)
            if isin not in used_isins:
                used_isins.add(isin)
                break
        
        # Generate instrument data
        instrument = {
            'isin': isin,
            'name': generate_instrument_name(),
            'long_name': generate_long_instrument_name(),
            'price': generate_price()
        }
        
        instruments.append(instrument)
        
        # Print progress
        if (i + 1) % 5000 == 0:
            print(f"Generated {i + 1} instruments...")
    
    return instruments

def save_to_csv(instruments, filename="instruments_test_data.csv"):
    """Save instruments data to CSV file"""
    df = pd.DataFrame(instruments)
    df.to_csv(filename, index=False)
    print(f"\nSaved {len(instruments)} instruments to '{filename}'")
    
    # Display sample data
    print("\nSample data:")
    print(df.head(10).to_string(index=False))
    
    # Display statistics
    print(f"\nData Statistics:")
    print(f"Total instruments: {len(df)}")
    print(f"Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
    print(f"Average price: ${df['price'].mean():.2f}")
    print(f"Unique countries: {len(set(isin[:2] for isin in df['isin']))}")

if __name__ == "__main__":
    print("Step 2: Generating test data for instruments")
    print("=" * 50)
    
    # Generate test data
    instruments = create_test_data(50000)  # Generate 50,000 test instruments
    
    # Save to CSV
    save_to_csv(instruments)
    
    print("\nTest data generation complete!")
    print("You can now view the 'instruments_test_data.csv' file") 