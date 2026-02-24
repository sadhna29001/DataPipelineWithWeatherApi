"""
Main Weather Data Pipeline
Orchestrates the ETL process for weather data
"""
import os
import sys
import yaml
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import PipelineLogger
from src.extract.weather_extractor import WeatherExtractor
from src.transform.weather_transformer import WeatherTransformer
from src.load.data_loader import DataLoader


class WeatherPipeline:
    """Main pipeline orchestrator for weather data"""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the pipeline
        
        Args:
            config_path: Path to configuration file
        """
        # Load environment variables
        load_dotenv()
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize logger
        log_dir = self.config.get('logging', {}).get('log_dir', './logs')
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.logger = PipelineLogger('weather_pipeline', log_dir, log_level).get_logger()
        
        # Initialize components
        self.extractor = None
        self.transformer = None
        self.loader = None
        
        self._initialize_components()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    
    def _initialize_components(self):
        """Initialize pipeline components"""
        # Get API credentials (RapidAPI)
        api_key = os.getenv('RAPIDAPI_KEY') or os.getenv('WEATHER_API_KEY')
        base_url = os.getenv('WEATHER_API_BASE_URL')
        api_host = os.getenv('RAPIDAPI_HOST', 'weatherapi-com.p.rapidapi.com')
        
        if not api_key:
            self.logger.error("RAPIDAPI_KEY not found in environment variables")
            raise ValueError("Missing API key - please add RAPIDAPI_KEY to .env file")
        
        # Initialize extractor with RapidAPI support
        self.extractor = WeatherExtractor(api_key, base_url, api_host, self.logger)
        
        # Initialize transformer
        self.transformer = WeatherTransformer(self.logger)
        
        # Initialize loader
        self.loader = DataLoader(self.logger)
        
        self.logger.info("Pipeline components initialized successfully")
    
    def run(self, cities: List[str] = None) -> bool:
        """
        Run the complete ETL pipeline
        
        Args:
            cities: List of cities to fetch weather for
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting Weather Data Pipeline")
            self.logger.info("=" * 60)
            
            # Get cities from parameter or environment
            if not cities:
                cities_str = os.getenv('CITIES', 'London,New York,Tokyo')
                cities = [city.strip() for city in cities_str.split(',')]
            
            self.logger.info(f"Fetching weather for {len(cities)} cities: {', '.join(cities)}")
            
            # EXTRACT: Fetch weather data
            self.logger.info("Phase 1: Data Extraction")
            raw_data = self.extractor.fetch_multiple_cities(cities)
            
            if not raw_data:
                self.logger.error("No data extracted, aborting pipeline")
                return False
            
            # TRANSFORM: Clean and transform data
            self.logger.info("Phase 2: Data Transformation")
            transformed_df = self.transformer.transform_weather_data(raw_data)
            
            if transformed_df.empty:
                self.logger.error("No data after transformation, aborting pipeline")
                return False
            
            # Add derived features
            transformed_df = self.transformer.add_derived_features(transformed_df)
            
            # LOAD: Save data to storage
            self.logger.info("Phase 3: Data Loading")
            storage_type = self.config.get('storage', {}).get('type', 'csv')
            
            success = False
            if storage_type == 'csv':
                csv_path = self.config.get('storage', {}).get('csv_path', './data/weather_data.csv')
                success = self.loader.load_to_csv(transformed_df, csv_path, mode='append')
            
            elif storage_type == 'sqlite':
                db_path = self.config.get('storage', {}).get('sqlite_path', './data/weather_data.db')
                success = self.loader.load_to_sqlite(transformed_df, db_path)
            
            elif storage_type == 'postgresql':
                # Build connection string from environment
                db_host = os.getenv('DB_HOST', 'localhost')
                db_port = os.getenv('DB_PORT', '5432')
                db_name = os.getenv('DB_NAME', 'weather_db')
                db_user = os.getenv('DB_USER', 'postgres')
                db_password = os.getenv('DB_PASSWORD', '')
                
                conn_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
                success = self.loader.load_to_postgresql(transformed_df, conn_str)
            
            else:
                self.logger.warning(f"Unknown storage type: {storage_type}, defaulting to CSV")
                success = self.loader.load_to_csv(transformed_df, './data/weather_data.csv')
            
            if success:
                self.logger.info("=" * 60)
                self.logger.info("Pipeline completed successfully!")
                self.logger.info(f"Processed {len(transformed_df)} weather records")
                self.logger.info("=" * 60)
                return True
            else:
                self.logger.error("Pipeline failed during data loading")
                return False
                
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            return False
        
        finally:
            # Cleanup
            if self.extractor:
                self.extractor.close()
    
    def run_with_stats(self, cities: List[str] = None):
        """Run pipeline and print statistics"""
        success = self.run(cities)
        
        if success:
            self._print_statistics()
        
        return success
    
    def _print_statistics(self):
        """Print pipeline statistics"""
        storage_type = self.config.get('storage', {}).get('type', 'csv')
        
        if storage_type == 'csv':
            csv_path = self.config.get('storage', {}).get('csv_path', './data/weather_data.csv')
            if os.path.exists(csv_path):
                import pandas as pd
                df = pd.read_csv(csv_path)
                self.logger.info(f"Total records in storage: {len(df)}")
                self.logger.info(f"Unique cities: {df['city'].nunique() if 'city' in df.columns else 'N/A'}")


def main():
    """Main entry point"""
    print("""
    ╔══════════════════════════════════════════════════════╗
    ║        Weather Data Pipeline - Python ETL            ║
    ║              Powered by RapidAPI Weather             ║
    ╚══════════════════════════════════════════════════════╝
    """)
    
    # Check if .env exists
    if not os.path.exists('.env'):
        print("⚠️  Warning: .env file not found!")
        print("Please create a .env file based on .env.example")
        print("You need to add your RapidAPI key for Weather API\n")
        return
    
    try:
        # Create and run pipeline
        pipeline = WeatherPipeline()
        pipeline.run_with_stats()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
