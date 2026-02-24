"""
Example: Data Analysis
Analyze weather data stored in the pipeline
"""
import os
import pandas as pd
from pathlib import Path

# Assuming data has been collected by the pipeline
data_file = './data/weather_data.csv'

if not os.path.exists(data_file):
    print("❌ No data file found. Run the pipeline first: python pipeline.py")
    exit(1)

# Load the data
print("📊 Loading weather data...")
df = pd.read_csv(data_file)

print(f"✅ Loaded {len(df)} records\n")

# Basic statistics
print("="*60)
print("WEATHER DATA ANALYSIS")
print("="*60)

print("\n1. DATA OVERVIEW")
print("-" * 60)
print(f"Total Records: {len(df)}")
print(f"Date Range: {df['data_timestamp'].min()} to {df['data_timestamp'].max()}")
print(f"Unique Cities: {df['city'].nunique()}")
print(f"Cities: {', '.join(df['city'].unique())}")

print("\n2. TEMPERATURE STATISTICS (°C)")
print("-" * 60)
print(f"Average: {df['temperature'].mean():.2f}")
print(f"Median: {df['temperature'].median():.2f}")
print(f"Min: {df['temperature'].min():.2f} ({df.loc[df['temperature'].idxmin(), 'city']})")
print(f"Max: {df['temperature'].max():.2f} ({df.loc[df['temperature'].idxmax(), 'city']})")
print(f"Std Dev: {df['temperature'].std():.2f}")

print("\n3. HUMIDITY STATISTICS (%)")
print("-" * 60)
print(f"Average: {df['humidity'].mean():.2f}")
print(f"Min: {df['humidity'].min():.2f} ({df.loc[df['humidity'].idxmin(), 'city']})")
print(f"Max: {df['humidity'].max():.2f} ({df.loc[df['humidity'].idxmax(), 'city']})")

print("\n4. WIND STATISTICS (m/s)")
print("-" * 60)
print(f"Average Speed: {df['wind_speed'].mean():.2f}")
print(f"Max Speed: {df['wind_speed'].max():.2f} ({df.loc[df['wind_speed'].idxmax(), 'city']})")

print("\n5. WEATHER CONDITIONS")
print("-" * 60)
weather_counts = df['weather_main'].value_counts()
for condition, count in weather_counts.items():
    print(f"{condition}: {count} ({count/len(df)*100:.1f}%)")

print("\n6. TOP 5 HOTTEST CITIES")
print("-" * 60)
hottest = df.groupby('city')['temperature'].mean().sort_values(ascending=False).head(5)
for city, temp in hottest.items():
    print(f"{city}: {temp:.2f}°C")

print("\n7. TOP 5 COLDEST CITIES")
print("-" * 60)
coldest = df.groupby('city')['temperature'].mean().sort_values().head(5)
for city, temp in coldest.items():
    print(f"{city}: {temp:.2f}°C")

print("\n8. MOST HUMID CITIES")
print("-" * 60)
humid = df.groupby('city')['humidity'].mean().sort_values(ascending=False).head(5)
for city, humidity in humid.items():
    print(f"{city}: {humidity:.2f}%")

print("\n" + "="*60)
print("Analysis complete!")
print("="*60)

# Export summary report
summary = {
    'total_records': len(df),
    'unique_cities': df['city'].nunique(),
    'avg_temperature': df['temperature'].mean(),
    'avg_humidity': df['humidity'].mean(),
    'avg_wind_speed': df['wind_speed'].mean(),
}

print(f"\n📄 Summary: {summary}")
