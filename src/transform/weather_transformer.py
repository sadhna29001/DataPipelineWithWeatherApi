"""
Weather data transformer - Cleans and transforms raw weather data
"""
import pandas as pd
from typing import Dict, List
from datetime import datetime


class WeatherTransformer:
    """Transform and clean weather data"""
    
    def __init__(self, logger=None):
        """
        Initialize the transformer
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
    
    def transform_weather_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Transform raw weather data into a structured DataFrame
        
        Args:
            raw_data: List of raw weather data dictionaries
            
        Returns:
            Transformed pandas DataFrame
        """
        if not raw_data:
            if self.logger:
                self.logger.warning("No data to transform")
            return pd.DataFrame()
        
        if self.logger:
            self.logger.info(f"Transforming {len(raw_data)} weather records")
        
        transformed_records = []
        
        for record in raw_data:
            try:
                transformed = self._extract_weather_fields(record)
                transformed_records.append(transformed)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error transforming record: {str(e)}")
                continue
        
        df = pd.DataFrame(transformed_records)
        
        # Perform data quality checks and cleaning
        df = self._clean_data(df)
        
        if self.logger:
            self.logger.info(f"Successfully transformed {len(df)} records")
        
        return df
    
    def _extract_weather_fields(self, record: Dict) -> Dict:
        """
        Extract relevant fields from raw weather data
        Supports both RapidAPI and OpenWeatherMap formats
        
        Args:
            record: Raw weather data dictionary
            
        Returns:
            Dictionary with extracted fields
        """
        # Detect API source
        source = record.get('source', 'openweathermap')
        
        if source == 'rapidapi':
            return self._extract_rapidapi_fields(record)
        else:
            return self._extract_openweathermap_fields(record)
    
    def _extract_rapidapi_fields(self, record: Dict) -> Dict:
        """Extract fields from RapidAPI WeatherAPI format"""
        location = record.get('location', {})
        current = record.get('current', {})
        condition = current.get('condition', {})
        
        # Parse timestamp
        epoch_time = current.get('last_updated_epoch', 0)
        data_timestamp = datetime.fromtimestamp(epoch_time) if epoch_time else datetime.now()
        
        transformed = {
            # Location
            'city': location.get('name', 'Unknown'),
            'country': location.get('country', 'Unknown'),
            'latitude': location.get('lat'),
            'longitude': location.get('lon'),
            'timezone': location.get('tz_id'),
            
            # Temperature (already in Celsius)
            'temperature': current.get('temp_c'),
            'feels_like': current.get('feelslike_c'),
            'temp_min': current.get('temp_c'),  # Current temp
            'temp_max': current.get('temp_c'),  # Current temp
            
            # Atmospheric conditions
            'pressure': current.get('pressure_mb'),
            'humidity': current.get('humidity'),
            'sea_level': current.get('pressure_mb'),
            'ground_level': current.get('pressure_mb'),
            
            # Weather description
            'weather_main': condition.get('text', ''),
            'weather_description': condition.get('text', ''),
            'weather_id': condition.get('code'),
            
            # Wind
            'wind_speed': current.get('wind_kph', 0) / 3.6,  # Convert kph to m/s
            'wind_direction': current.get('wind_degree'),
            'wind_gust': current.get('gust_kph', 0) / 3.6,  # Convert kph to m/s
            
            # Clouds
            'cloudiness': current.get('cloud'),
            
            # Visibility (convert km to meters)
            'visibility': current.get('vis_km', 0) * 1000,
            
            # Additional RapidAPI specific fields
            'uv_index': current.get('uv'),
            'air_quality_co': current.get('air_quality', {}).get('co'),
            'air_quality_pm25': current.get('air_quality', {}).get('pm2_5'),
            
            # Timestamps
            'data_timestamp': data_timestamp,
            'sunrise': None,  # Not in current weather endpoint
            'sunset': None,   # Not in current weather endpoint
            'extracted_at': record.get('extracted_at'),
            
            # Metadata
            'source': 'rapidapi',
            'api_host': record.get('api_host', 'weatherapi-com.p.rapidapi.com')
        }
        
        return transformed
    
    def _extract_openweathermap_fields(self, record: Dict) -> Dict:
        """Extract fields from OpenWeatherMap format"""
        # Extract main fields
        main = record.get('main', {})
        weather = record.get('weather', [{}])[0]
        wind = record.get('wind', {})
        clouds = record.get('clouds', {})
        sys = record.get('sys', {})
        coord = record.get('coord', {})
        
        transformed = {
            # Location
            'city': record.get('name', 'Unknown'),
            'country': sys.get('country', 'Unknown'),
            'latitude': coord.get('lat'),
            'longitude': coord.get('lon'),
            
            # Temperature
            'temperature': main.get('temp'),
            'feels_like': main.get('feels_like'),
            'temp_min': main.get('temp_min'),
            'temp_max': main.get('temp_max'),
            
            # Atmospheric conditions
            'pressure': main.get('pressure'),
            'humidity': main.get('humidity'),
            'sea_level': main.get('sea_level'),
            'ground_level': main.get('grnd_level'),
            
            # Weather description
            'weather_main': weather.get('main'),
            'weather_description': weather.get('description'),
            'weather_id': weather.get('id'),
            
            # Wind
            'wind_speed': wind.get('speed'),
            'wind_direction': wind.get('deg'),
            'wind_gust': wind.get('gust'),
            
            # Clouds
            'cloudiness': clouds.get('all'),
            
            # Visibility
            'visibility': record.get('visibility'),
            
            # Timestamps
            'data_timestamp': datetime.fromtimestamp(record.get('dt', 0)),
            'sunrise': datetime.fromtimestamp(sys.get('sunrise', 0)),
            'sunset': datetime.fromtimestamp(sys.get('sunset', 0)),
            'extracted_at': record.get('extracted_at'),
            
            # Metadata
            'source': record.get('source', 'openweathermap')
        }
        
        return transformed
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate the DataFrame
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['city', 'data_timestamp'], keep='last')
        
        if self.logger and len(df) < initial_count:
            self.logger.info(f"Removed {initial_count - len(df)} duplicate records")
        
        # Handle missing values
        # For numeric columns, we can fill with median or leave as NaN
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        
        # Sort by timestamp
        if 'data_timestamp' in df.columns:
            df = df.sort_values('data_timestamp', ascending=False)
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived/calculated features to the dataset
        
        Args:
            df: DataFrame with weather data
            
        Returns:
            DataFrame with additional features
        """
        if df.empty:
            return df
        
        # Temperature range
        if 'temp_max' in df.columns and 'temp_min' in df.columns:
            df['temp_range'] = df['temp_max'] - df['temp_min']
        
        # Heat index category
        if 'temperature' in df.columns:
            df['temp_category'] = pd.cut(
                df['temperature'],
                bins=[-float('inf'), 0, 10, 20, 30, float('inf')],
                labels=['Freezing', 'Cold', 'Moderate', 'Warm', 'Hot']
            )
        
        # Humidity category
        if 'humidity' in df.columns:
            df['humidity_category'] = pd.cut(
                df['humidity'],
                bins=[0, 30, 60, 100],
                labels=['Low', 'Moderate', 'High']
            )
        
        # Wind speed category (Beaufort scale simplified)
        if 'wind_speed' in df.columns:
            df['wind_category'] = pd.cut(
                df['wind_speed'],
                bins=[0, 1, 5, 10, 20, float('inf')],
                labels=['Calm', 'Light', 'Moderate', 'Strong', 'Very Strong']
            )
        
        if self.logger:
            self.logger.info("Added derived features to dataset")
        
        return df
    
    def aggregate_data(self, df: pd.DataFrame, group_by: str = 'city') -> pd.DataFrame:
        """
        Aggregate weather data by specified column
        
        Args:
            df: DataFrame with weather data
            group_by: Column to group by
            
        Returns:
            Aggregated DataFrame
        """
        if df.empty or group_by not in df.columns:
            return df
        
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        
        agg_funcs = {col: ['mean', 'min', 'max', 'std'] for col in numeric_cols}
        
        aggregated = df.groupby(group_by).agg(agg_funcs)
        aggregated.columns = ['_'.join(col).strip() for col in aggregated.columns.values]
        aggregated = aggregated.reset_index()
        
        if self.logger:
            self.logger.info(f"Aggregated data by {group_by}")
        
        return aggregated
