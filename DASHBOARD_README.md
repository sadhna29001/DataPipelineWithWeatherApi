# Weather Data Pipeline - Web Dashboard

## Overview
A modern web-based dashboard for monitoring and managing your weather data pipeline. View real-time weather data, monitor pipeline status, and trigger pipeline runs from a beautiful, responsive interface.

## Features

### 🌟 Dashboard Features
- **Real-time Pipeline Monitoring**: See pipeline status, last run time, and records processed
- **Weather Data Visualization**: View latest weather data in beautiful card layouts
- **Interactive Controls**: Run pipeline with a single click
- **Live Statistics**: Track temperature, humidity, and other metrics
- **Log Viewer**: Monitor pipeline logs in real-time
- **Search & Filter**: Quickly find weather data for specific cities
- **Auto-refresh**: Dashboard automatically updates every 5 seconds

### 📊 Data Display
- Current weather conditions for multiple cities
- Temperature, humidity, wind speed, and pressure
- Weather descriptions and conditions
- City and country information
- Last updated timestamps

## Installation

### 1. Install Python Dependencies

Make sure you have Python 3.8+ installed, then install the required packages:

```bash
# If you have a virtual environment
source venv/bin/activate  # or .venv/bin/activate

# Install packages
pip install flask pandas pyyaml python-dotenv requests
```

Or install all requirements:
```bash
pip install -r requirements.txt
```

### 2. Environment Configuration

Make sure your `.env` file is configured with your RapidAPI credentials:

```env
RAPIDAPI_KEY=your_rapidapi_key_here
RAPIDAPI_HOST=weatherapi-com.p.rapidapi.com
CITIES=London,New York,Tokyo,Paris,Sydney
```

## Running the Dashboard

### Start the Web Server

```bash
python app.py
```

Or with Python 3:
```bash
python3 app.py
```

The dashboard will be available at: **http://localhost:5000**

### Alternative: Run in Background

```bash
python app.py > dashboard.log 2>&1 &
```

## Using the Dashboard

### 1. Access the Dashboard
Open your web browser and navigate to `http://localhost:5000`

### 2. Run the Pipeline
- Click the **"Run Pipeline"** button in the top right
- Watch the status bar update in real-time
- Pipeline status will change from "Idle" → "Running" → "Success"

### 3. View Weather Data
- **Weather Data Tab**: View latest weather for each city in card format
- Use the search box to filter cities
- Cards show temperature, humidity, wind, pressure, and conditions

### 4. Monitor Logs
- Click on the **"Logs"** tab
- View real-time pipeline execution logs
- Click "Refresh Logs" to update

### 5. View Statistics
- Click on the **"Statistics"** tab
- See aggregate statistics across all cities
- Temperature and humidity ranges
- List of monitored cities

## Dashboard Components

### Status Bar
Shows real-time pipeline information:
- Current status (Idle/Running/Success/Error)
- Last run timestamp
- Total records processed
- Status message

### Statistics Cards
Quick overview metrics:
- Total records in database
- Average temperature across cities
- Average humidity
- Number of cities monitored

### Tabs
1. **Weather Data**: Latest weather information with search
2. **Logs**: Pipeline execution logs
3. **Statistics**: Detailed analytics and metrics

## API Endpoints

The dashboard exposes the following REST API endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard UI |
| `/api/status` | GET | Get pipeline status |
| `/api/run` | POST | Trigger pipeline execution |
| `/api/data` | GET | Get all weather data |
| `/api/data/latest` | GET | Get latest data per city |
| `/api/logs` | GET | Get recent log entries |
| `/api/stats` | GET | Get pipeline statistics |

### Example API Usage

```bash
# Get pipeline status
curl http://localhost:5000/api/status

# Run pipeline
curl -X POST http://localhost:5000/api/run

# Get latest weather data
curl http://localhost:5000/api/data/latest
```

## Troubleshooting

### Port Already in Use
If port 5000 is already in use, modify `app.py`:
```python
app.run(debug=True, host='0.0.0.0', port=8080)  # Change port
```

### No Data Available
1. Make sure the pipeline has run at least once
2. Check that `data/weather_data.csv` exists
3. Verify API credentials in `.env`
4. Check logs for errors

### Flask Not Installed
```bash
pip install flask
```

### Cannot Connect to API
1. Verify RAPIDAPI_KEY in `.env`
2. Check internet connection
3. Verify API host is correct

## Development

### File Structure
```
DataPipelne/
├── app.py                 # Flask application
├── templates/
│   └── index.html        # Dashboard HTML
├── static/
│   ├── css/
│   │   └── style.css     # Dashboard styles
│   └── js/
│       └── app.js        # Dashboard JavaScript
├── data/
│   └── weather_data.csv  # Weather data storage
├── logs/
│   └── pipeline.log      # Application logs
└── pipeline.py           # Pipeline orchestrator
```

### Customization

#### Change Refresh Interval
Edit `static/js/app.js`:
```javascript
// Change from 5000ms (5s) to desired interval
refreshInterval = setInterval(() => {
    updateStatus();
}, 5000);  // milliseconds
```

#### Modify Color Scheme
Edit `static/css/style.css`:
```css
:root {
    --primary-color: #2c3e50;    /* Main color */
    --secondary-color: #3498db;  /* Accent color */
    /* ... modify other colors */
}
```

#### Add New Cities
Update `.env`:
```env
CITIES=London,New York,Tokyo,Paris,Sydney,Berlin,Mumbai
```

## Production Deployment

For production use, consider:

1. **Use a Production Server**
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

2. **Enable HTTPS**
Use nginx or Apache as a reverse proxy with SSL

3. **Set Production Secret Key**
Update `.env`:
```env
SECRET_KEY=your-secure-random-secret-key-here
```

4. **Disable Debug Mode**
In `app.py`:
```python
app.run(debug=False, host='0.0.0.0', port=5000)
```

## Screenshots

The dashboard features:
- 📱 Responsive design (works on mobile, tablet, desktop)
- 🎨 Modern, gradient-based UI
- 🔄 Real-time updates
- 🎯 Intuitive navigation
- 📊 Visual data representation

## Support

For issues or questions:
1. Check the logs: `logs/pipeline.log`
2. Verify environment configuration
3. Ensure all dependencies are installed
4. Check API credentials and limits

## License

Part of the Weather Data Pipeline project.
