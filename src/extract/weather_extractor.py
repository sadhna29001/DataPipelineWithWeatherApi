"""
Weather data extractor - Fetches data from RapidAPI Weather APIs
Supports multiple RapidAPI weather endpoints including WeatherAPI
"""
import os
import time
import requests
from typing import Dict, List, Optional
from datetime import datetime


class WeatherExtractor:
    """Extract weather data from RapidAPI Weather APIs"""
    
    def __init__(self, api_key: str, base_url: str, api_host: str = None, logger=None):
        """
        Initialize the weather extractor
        
        Args:
            api_key: RapidAPI key
            base_url: Base URL for the API
            api_host: RapidAPI host (e.g., 'weatherapi-com.p.rapidapi.com')
            logger: Logger instance
        """
        self.api_key = api_key
        self.base_url = base_url
        self.api_host = api_host or 'weatherapi-com.p.rapidapi.com'
        self.logger = logger
        self.session = requests.Session()
        
        # Set RapidAPI headers
        self.headers = {
            'X-RapidAPI-Key': self.api_key,
            'X-RapidAPI-Host': self.api_host
        }
    
    def fetch_weather(self, city: str, retry_attempts: int = 3) -> Optional[Dict]:
        """
        Fetch current weather data for a city
        
        Args:
            city: City name
            retry_attempts: Number of retry attempts on failure
            
        Returns:
            Dictionary containing weather data or None on failure
        """
        endpoint = f"{self.base_url}/current.json"
        params = {
            'q': city
        }
        
        for attempt in range(retry_attempts):
            try:
                if self.logger:
                    self.logger.info(f"Fetching weather data for {city} (attempt {attempt + 1}/{retry_attempts})")
                
                response = self.session.get(endpoint, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Add extraction metadata
                data['extracted_at'] = datetime.utcnow().isoformat()
                data['source'] = 'rapidapi'
                data['api_host'] = self.api_host
                
                if self.logger:
                    self.logger.info(f"Successfully fetched weather data for {city}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error fetching weather for {city}: {str(e)}")
                
                if attempt < retry_attempts - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    if self.logger:
                        self.logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    if self.logger:
                        self.logger.error(f"Failed to fetch weather for {city} after {retry_attempts} attempts")
                    return None
        
        return None
    
    def fetch_multiple_cities(self, cities: List[str]) -> List[Dict]:
        """
        Fetch weather data for multiple cities
        
        Args:
            cities: List of city names
            
        Returns:
            List of weather data dictionaries
        """
        results = []
        
        for city in cities:
            data = self.fetch_weather(city.strip())
            if data:
                results.append(data)
            
            # Be respectful to the API - add small delay between requests
            time.sleep(0.5)
        
        if self.logger:
            self.logger.info(f"Extracted weather data for {len(results)}/{len(cities)} cities")
        
        return results
    
    def fetch_forecast(self, city: str, days: int = 5, retry_attempts: int = 3) -> Optional[Dict]:
        """
        Fetch weather forecast for a city
        
        Args:
            city: City name
            days: Number of forecast days (1-10)
            retry_attempts: Number of retry attempts on failure
            
        Returns:
            Dictionary containing forecast data or None on failure
        """
        endpoint = f"{self.base_url}/forecast.json"
        params = {
            'q': city,
            'days': min(days, 10)  # Max 10 days
        }
        
        for attempt in range(retry_attempts):
            try:
                if self.logger:
                    self.logger.info(f"Fetching forecast for {city}")
                
                response = self.session.get(endpoint, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                data['extracted_at'] = datetime.utcnow().isoformat()
                data['source'] = 'rapidapi'
                
                return data
                
            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error fetching forecast for {city}: {str(e)}")
                
                if attempt < retry_attempts - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        
        return None
    
    def fetch_astronomy(self, city: str, retry_attempts: int = 3) -> Optional[Dict]:
        """
        Fetch astronomy data (sunrise, sunset, moon phases)
        
        Args:
            city: City name
            retry_attempts: Number of retry attempts on failure
            
        Returns:
            Dictionary containing astronomy data or None on failure
        """
        endpoint = f"{self.base_url}/astronomy.json"
        params = {
            'q': city
        }
        
        for attempt in range(retry_attempts):
            try:
                if self.logger:
                    self.logger.info(f"Fetching astronomy data for {city}")
                
                response = self.session.get(endpoint, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                data['extracted_at'] = datetime.utcnow().isoformat()
                data['source'] = 'rapidapi'
                
                return data
                
            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error fetching astronomy for {city}: {str(e)}")
                
                if attempt < retry_attempts - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        
        return None
    
    def search_location(self, query: str, retry_attempts: int = 3) -> Optional[Dict]:
        """
        Search for location information
        
        Args:
            query: Search query (city name, coordinates, postal code)
            retry_attempts: Number of retry attempts on failure
            
        Returns:
            Dictionary containing location data or None on failure
        """
        endpoint = f"{self.base_url}/search.json"
        params = {
            'q': query
        }
        
        for attempt in range(retry_attempts):
            try:
                if self.logger:
                    self.logger.info(f"Searching location: {query}")
                
                response = self.session.get(endpoint, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                return data
                
            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error searching location {query}: {str(e)}")
                
                if attempt < retry_attempts - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        
        return None
    
    def close(self):
        """Close the session"""
        self.session.close()
