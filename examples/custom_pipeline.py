"""
Example: Custom Pipeline Usage
Demonstrates how to use pipeline components individually
"""
import os
from dotenv import load_dotenv
from src.extract.weather_extractor import WeatherExtractor
from src.transform.weather_transformer import WeatherTransformer
from src.load.data_loader import DataLoader
from src.utils.logger import PipelineLogger

# Load environment variables
load_dotenv()

# Initialize logger
logger = PipelineLogger('custom_example').get_logger()

# Get API credentials (RapidAPI)
api_key = os.getenv('RAPIDAPI_KEY')
base_url = os.getenv('WEATHER_API_BASE_URL')
api_host = os.getenv('RAPIDAPI_HOST', 'weatherapi-com.p.rapidapi.com')

logger.info("Starting custom pipeline example with RapidAPI")

# Step 1: Extract weather data
logger.info("Extracting weather data...")
extractor = WeatherExtractor(api_key, base_url, api_host, logger)

cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata']
raw_data = extractor.fetch_multiple_cities(cities)

logger.info(f"Extracted data for {len(raw_data)} cities")

# Step 2: Transform data
logger.info("Transforming data...")
transformer = WeatherTransformer(logger)

# Basic transformation
df = transformer.transform_weather_data(raw_data)

# Add derived features
df = transformer.add_derived_features(df)

logger.info(f"Transformed {len(df)} records")

# Display some statistics
if not df.empty:
    print("\n" + "="*60)
    print("Weather Data Summary")
    print("="*60)
    print(f"\nCities: {', '.join(df['city'].tolist())}")
    print(f"\nAverage Temperature: {df['temperature'].mean():.2f}°C")
    print(f"Max Temperature: {df['temperature'].max():.2f}°C")
    print(f"Min Temperature: {df['temperature'].min():.2f}°C")
    print(f"\nAverage Humidity: {df['humidity'].mean():.2f}%")
    print(f"\nAverage Wind Speed: {df['wind_speed'].mean():.2f} m/s")
    print("\n" + "="*60)

# Step 3: Load data to multiple destinations
logger.info("Loading data to storage...")
loader = DataLoader(logger)

# Save to CSV
loader.load_to_csv(df, './data/custom_weather.csv', mode='overwrite')

# Save to JSON
loader.load_to_json(df, './data/custom_weather.json')

# Save to SQLite
loader.load_to_sqlite(df, './data/custom_weather.db', table_name='indian_cities')

# Create backup
backup_file = loader.create_backup(df, './backups')

logger.info("Custom pipeline completed successfully!")

# Cleanup
extractor.close()

print("\n✅ Custom pipeline example completed!")
print("📁 Check the data/ and backups/ directories for output files")
