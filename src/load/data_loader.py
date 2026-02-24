"""
Data loader - Load transformed data into various storage systems
"""
import os
import pandas as pd
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine
from datetime import datetime


class DataLoader:
    """Load data into various storage backends"""
    
    def __init__(self, logger=None):
        """
        Initialize the data loader
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
    
    def load_to_csv(self, df: pd.DataFrame, file_path: str, mode: str = 'append') -> bool:
        """
        Load data to CSV file
        
        Args:
            df: DataFrame to save
            file_path: Path to CSV file
            mode: 'append' or 'overwrite'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            if mode == 'append' and os.path.exists(file_path):
                # Append to existing file
                df.to_csv(file_path, mode='a', header=False, index=False)
                if self.logger:
                    self.logger.info(f"Appended {len(df)} records to {file_path}")
            else:
                # Overwrite or create new file
                df.to_csv(file_path, mode='w', header=True, index=False)
                if self.logger:
                    self.logger.info(f"Saved {len(df)} records to {file_path}")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving to CSV: {str(e)}")
            return False
    
    def load_to_sqlite(self, df: pd.DataFrame, db_path: str, table_name: str = 'weather_data',
                       if_exists: str = 'append') -> bool:
        """
        Load data to SQLite database
        
        Args:
            df: DataFrame to save
            db_path: Path to SQLite database file
            table_name: Name of the table
            if_exists: 'fail', 'replace', or 'append'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Create SQLite engine
            engine = create_engine(f'sqlite:///{db_path}')
            
            # Load data
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            
            if self.logger:
                self.logger.info(f"Loaded {len(df)} records to SQLite table '{table_name}'")
            
            engine.dispose()
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error loading to SQLite: {str(e)}")
            return False
    
    def load_to_postgresql(self, df: pd.DataFrame, connection_string: str,
                          table_name: str = 'weather_data', if_exists: str = 'append') -> bool:
        """
        Load data to PostgreSQL database
        
        Args:
            df: DataFrame to save
            connection_string: PostgreSQL connection string
            table_name: Name of the table
            if_exists: 'fail', 'replace', or 'append'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create PostgreSQL engine
            engine = create_engine(connection_string)
            
            # Load data
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            
            if self.logger:
                self.logger.info(f"Loaded {len(df)} records to PostgreSQL table '{table_name}'")
            
            engine.dispose()
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error loading to PostgreSQL: {str(e)}")
            return False
    
    def load_to_json(self, df: pd.DataFrame, file_path: str, orient: str = 'records') -> bool:
        """
        Load data to JSON file
        
        Args:
            df: DataFrame to save
            file_path: Path to JSON file
            orient: JSON orientation ('records', 'index', 'columns', etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Convert datetime columns to string for JSON serialization
            df_copy = df.copy()
            for col in df_copy.select_dtypes(include=['datetime64']).columns:
                df_copy[col] = df_copy[col].astype(str)
            
            # Save to JSON
            df_copy.to_json(file_path, orient=orient, indent=2)
            
            if self.logger:
                self.logger.info(f"Saved {len(df)} records to {file_path}")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving to JSON: {str(e)}")
            return False
    
    def load_to_parquet(self, df: pd.DataFrame, file_path: str) -> bool:
        """
        Load data to Parquet file (efficient columnar format)
        
        Args:
            df: DataFrame to save
            file_path: Path to Parquet file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Save to Parquet
            df.to_parquet(file_path, index=False)
            
            if self.logger:
                self.logger.info(f"Saved {len(df)} records to {file_path}")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving to Parquet: {str(e)}")
            return False
    
    def create_backup(self, df: pd.DataFrame, backup_dir: str = './backups') -> Optional[str]:
        """
        Create a timestamped backup of the data
        
        Args:
            df: DataFrame to backup backup_dir: Directory for backups
            
        Returns:
            Path to backup file or None on failure
        """
        try:
            Path(backup_dir).mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"{backup_dir}/weather_backup_{timestamp}.csv"
            
            df.to_csv(backup_file, index=False)
            
            if self.logger:
                self.logger.info(f"Created backup: {backup_file}")
            
            return backup_file
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error creating backup: {str(e)}")
            return None
