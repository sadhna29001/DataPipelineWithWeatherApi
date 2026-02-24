"""
Example: Testing Individual Components
Unit-style tests for pipeline components
"""
import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract.weather_extractor import WeatherExtractor
from src.transform.weather_transformer import WeatherTransformer
from src.load.data_loader import DataLoader
from src.utils.logger import PipelineLogger

load_dotenv()

def test_extractor():
    """Test the weather extractor"""
    print("\n" + "="*60)
    print("Testing Weather Extractor (RapidAPI)")
    print("="*60)
    
    api_key = os.getenv('RAPIDAPI_KEY')
    base_url = os.getenv('WEATHER_API_BASE_URL')
    api_host = os.getenv('RAPIDAPI_HOST', 'weatherapi-com.p.rapidapi.com')
    
    extractor = WeatherExtractor(api_key, base_url, api_host)
    
    # Test single city
    print("\n1. Testing single city extraction (London)...")
    data = extractor.fetch_weather('London')
    
    if data and 'location' in data:
        print(f"   ✅ Success! Got data for {data['location']['name']}")
        print(f"   Temperature: {data['current']['temp_c']}°C")
    else:
        print("   ❌ Failed to fetch data")
        return False
    
    # Test multiple cities
    print("\n2. Testing multiple cities extraction...")
    cities = ['Paris', 'Tokyo', 'New York']
    results = extractor.fetch_multiple_cities(cities)
    
    if len(results) == len(cities):
        print(f"   ✅ Success! Got data for all {len(cities)} cities")
    else:
        print(f"   ⚠️  Partial success. Got {len(results)}/{len(cities)} cities")
    
    extractor.close()
    return True


def test_transformer():
    """Test the weather transformer"""
    print("\n" + "="*60)
    print("Testing Weather Transformer (RapidAPI Format)")
    print("="*60)
    
    # Create sample RapidAPI data
    sample_data = [{
        'location': {
            'name': 'TestCity',
            'country': 'Test Country',
            'lat': 51.5,
            'lon': -0.1,
            'tz_id': 'Europe/London'
        },
        'current': {
            'temp_c': 15.0,
            'feelslike_c': 14.0,
            'humidity': 65,
            'pressure_mb': 1013,
            'wind_kph': 18.0,
            'wind_degree': 180,
            'cloud': 25,
            'vis_km': 10,
            'uv': 5,
            'last_updated_epoch': 1708790400,
            'condition': {
                'text': 'Partly cloudy',
                'code': 1003
            }
        },
        'extracted_at': '2026-02-24T10:30:00',
        'source': 'rapidapi',
        'api_host': 'weatherapi-com.p.rapidapi.com'
    }]
    
    transformer = WeatherTransformer()
    
    print("\n1. Testing data transformation...")
    df = transformer.transform_weather_data(sample_data)
    
    if not df.empty and 'city' in df.columns:
        print(f"   ✅ Success! Transformed {len(df)} records")
        print(f"   Columns: {len(df.columns)}")
        print(f"   City: {df['city'].iloc[0]}")
    else:
        print("   ❌ Transformation failed")
        return False
    
    print("\n2. Testing derived features...")
    df = transformer.add_derived_features(df)
    
    if 'temp_category' in df.columns:
        print(f"   ✅ Success! Added derived features")
        print(f"   Temperature category: {df['temp_category'].iloc[0]}")
    else:
        print("   ❌ Failed to add derived features")
        return False
    
    return True


def test_loader():
    """Test the data loader"""
    print("\n" + "="*60)
    print("Testing Data Loader")
    print("="*60)
    
    import pandas as pd
    
    # Create sample DataFrame
    sample_df = pd.DataFrame({
        'city': ['TestCity'],
        'temperature': [15.0],
        'humidity': [65],
        'timestamp': ['2026-02-24 10:30:00']
    })
    
    loader = DataLoader()
    
    # Test CSV
    print("\n1. Testing CSV export...")
    success = loader.load_to_csv(sample_df, './data/test_output.csv', mode='overwrite')
    if success:
        print("   ✅ CSV export successful")
    else:
        print("   ❌ CSV export failed")
        return False
    
    # Test SQLite
    print("\n2. Testing SQLite export...")
    success = loader.load_to_sqlite(sample_df, './data/test_output.db', 'test_table')
    if success:
        print("   ✅ SQLite export successful")
    else:
        print("   ❌ SQLite export failed")
        return False
    
    # Test JSON
    print("\n3. Testing JSON export...")
    success = loader.load_to_json(sample_df, './data/test_output.json')
    if success:
        print("   ✅ JSON export successful")
    else:
        print("   ❌ JSON export failed")
        return False
    
    return True


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("PIPELINE COMPONENT TESTS")
    print("="*60)
    
    results = []
    
    # Test extractor
    try:
        results.append(('Extractor', test_extractor()))
    except Exception as e:
        print(f"❌ Extractor test failed: {str(e)}")
        results.append(('Extractor', False))
    
    # Test transformer
    try:
        results.append(('Transformer', test_transformer()))
    except Exception as e:
        print(f"❌ Transformer test failed: {str(e)}")
        results.append(('Transformer', False))
    
    # Test loader
    try:
        results.append(('Loader', test_loader()))
    except Exception as e:
        print(f"❌ Loader test failed: {str(e)}")
        results.append(('Loader', False))
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for component, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{component}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\nResults: {passed}/{total} tests passed")
    print("="*60)
    
    return passed == total


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
