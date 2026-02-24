"""
View Weather Data - Quick data viewer after pipeline runs
Run this after fetching data with: python3 pipeline.py
"""
import pandas as pd
import os

# Check if data file exists
data_file = 'data/weather_data.csv'

if not os.path.exists(data_file):
    print("❌ No data found!")
    print("Run the pipeline first: python3 pipeline.py")
    exit(1)

# Read the CSV data
print("📊 Loading weather data...\n")
df = pd.read_csv(data_file)

# Display basic info
print("=" * 80)
print("🌍 WEATHER DATA OVERVIEW")
print("=" * 80)
print(f"\n📈 Total Records: {len(df)}")
print(f"📅 Data Collection Time: {df['data_timestamp'].iloc[0] if len(df) > 0 else 'N/A'}")
print(f"🌍 Cities Covered: {df['city'].nunique()}")
print(f"🗺️  Countries: {df['country'].nunique()}")

# Temperature Statistics
print("\n" + "=" * 80)
print("🌡️  TEMPERATURE STATISTICS (°C)")
print("=" * 80)
print(f"Average: {df['temperature'].mean():.2f}°C")
print(f"Minimum: {df['temperature'].min():.2f}°C ({df.loc[df['temperature'].idxmin(), 'city']})")
print(f"Maximum: {df['temperature'].max():.2f}°C ({df.loc[df['temperature'].idxmax(), 'city']})")
print(f"Range: {df['temperature'].max() - df['temperature'].min():.2f}°C")

# Humidity & Wind
print("\n" + "=" * 80)
print("💧 HUMIDITY & WIND")
print("=" * 80)
print(f"Average Humidity: {df['humidity'].mean():.1f}%")
print(f"Average Wind Speed: {df['wind_speed'].mean():.2f} m/s")
print(f"Max Wind Gust: {df['wind_gust'].max():.2f} m/s")

# City-by-City Details
print("\n" + "=" * 80)
print("📍 CITY-BY-CITY WEATHER")
print("=" * 80)
print(f"\n{'City':<15} {'Temperature':<12} {'Feels Like':<12} {'Weather':<20} {'Humidity':<10}")
print("-" * 80)

for idx, row in df.iterrows():
    print(f"{row['city']:<15} {row['temperature']:>6.1f}°C     "
          f"{row['feels_like']:>6.1f}°C     "
          f"{row['weather_main']:<20} {row['humidity']:>4.0f}%")

# Weather Conditions Summary
print("\n" + "=" * 80)
print("☁️  WEATHER CONDITIONS")
print("=" * 80)
weather_counts = df['weather_main'].value_counts()
for weather, count in weather_counts.items():
    percentage = (count / len(df)) * 100
    print(f"{weather:<20} {count:>3} cities ({percentage:>5.1f}%)")

# Display full dataframe (first 10 columns for readability)
print("\n" + "=" * 80)
print("📋 DETAILED DATA (Key Columns)")
print("=" * 80)
display_cols = ['city', 'temperature', 'feels_like', 'humidity', 
                'wind_speed', 'weather_main', 'cloudiness', 'visibility']
print(df[display_cols].to_string(index=False))

# Save summary to text file
summary_file = 'data/weather_summary.txt'
with open(summary_file, 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("WEATHER DATA SUMMARY\n")
    f.write("=" * 80 + "\n\n")
    f.write(f"Total Records: {len(df)}\n")
    f.write(f"Cities: {', '.join(df['city'].unique())}\n\n")
    f.write("Temperature Statistics:\n")
    f.write(f"  Average: {df['temperature'].mean():.2f}°C\n")
    f.write(f"  Min: {df['temperature'].min():.2f}°C\n")
    f.write(f"  Max: {df['temperature'].max():.2f}°C\n\n")
    f.write(df[display_cols].to_string(index=False))

print("\n" + "=" * 80)
print(f"✅ Summary saved to: {summary_file}")
print("=" * 80)

# Optional: Show additional statistics
print("\n💡 TIP: For more analysis, run: python3 examples/analyze_data.py")
print("📊 TIP: View raw CSV: cat data/weather_data.csv")
print("🔍 TIP: Filter data in pandas:")
print("       python3 -c \"import pandas as pd; df = pd.read_csv('data/weather_data.csv'); print(df[df['temperature'] > 20])\"")
