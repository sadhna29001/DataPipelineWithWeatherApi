"""
Web Dashboard for Weather Data Pipeline
Flask application providing UI for monitoring and viewing pipeline data
"""
import os
import sys
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, jsonify, request
from threading import Thread
import time
import subprocess

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import WeatherPipeline

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')

# Global variables for pipeline status
pipeline_status = {
    'status': 'idle',  # idle, running, success, error
    'last_run': None,
    'message': 'Pipeline not yet run',
    'records_processed': 0
}


def run_pipeline_async(cities=None):
    """Run pipeline in background thread"""
    global pipeline_status
    try:
        pipeline_status['status'] = 'running'
        pipeline_status['message'] = 'Pipeline is running...'
        
        pipeline = WeatherPipeline()
        success = pipeline.run(cities)
        
        if success:
            pipeline_status['status'] = 'success'
            pipeline_status['message'] = 'Pipeline completed successfully'
            pipeline_status['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Count records
            csv_path = './data/weather_data.csv'
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                pipeline_status['records_processed'] = len(df)
        else:
            pipeline_status['status'] = 'error'
            pipeline_status['message'] = 'Pipeline failed - check logs'
            
    except Exception as e:
        pipeline_status['status'] = 'error'
        pipeline_status['message'] = f'Error: {str(e)}'


@app.route('/')
def index():
    """Render main dashboard"""
    return render_template('index.html')


@app.route('/api/status')
def get_status():
    """Get current pipeline status"""
    return jsonify(pipeline_status)


@app.route('/api/run', methods=['POST'])
def run_pipeline():
    """Trigger pipeline execution"""
    global pipeline_status
    
    # Reset status if it's stuck in error state
    if pipeline_status['status'] == 'error':
        pipeline_status['status'] = 'idle'
    
    if pipeline_status['status'] == 'running':
        return jsonify({
            'success': False,
            'message': 'Pipeline is already running'
        }), 400
    
    # Get cities from request or use defaults
    data = request.get_json(silent=True) or {}
    cities = data.get('cities', None)
    
    # Run pipeline in background thread
    thread = Thread(target=run_pipeline_async, args=(cities,))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Pipeline started'
    })


@app.route('/api/data')
def get_data():
    """Get latest weather data"""
    try:
        csv_path = './data/weather_data.csv'
        
        if not os.path.exists(csv_path):
            return jsonify({
                'success': False,
                'message': 'No data available. Run the pipeline first.'
            }), 404
        
        # Read CSV data
        df = pd.read_csv(csv_path)
        
        # Convert to records (convert numpy types to Python types)
        # Replace NaN with None for proper JSON serialization
        df = df.fillna('')  # Replace NaN with empty string
        records = df.to_dict('records')
        
        # Get summary statistics (convert numpy types to Python types)
        summary = {
            'total_records': int(len(df)),
            'cities': int(df['city'].nunique()) if 'city' in df.columns else 0,
            'last_update': str(df['extracted_at'].max()) if 'extracted_at' in df.columns else 'Unknown',
            'avg_temperature': float(round(df['temperature'].mean(), 1)) if 'temperature' in df.columns else 0,
            'avg_humidity': float(round(df['humidity'].mean(), 1)) if 'humidity' in df.columns else 0
        }
        
        return jsonify({
            'success': True,
            'data': records,
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error loading data: {str(e)}'
        }), 500


@app.route('/api/data/latest')
def get_latest_data():
    """Get only the latest data for each city"""
    try:
        csv_path = './data/weather_data.csv'
        
        if not os.path.exists(csv_path):
            return jsonify({
                'success': False,
                'message': 'No data available'
            }), 404
        
        df = pd.read_csv(csv_path)
        
        # Get latest record for each city
        if 'extracted_at' in df.columns and 'city' in df.columns:
            df['extracted_at'] = pd.to_datetime(df['extracted_at'])
            latest_df = df.sort_values('extracted_at').groupby('city').tail(1)
            # Replace NaN with empty string for proper JSON serialization
            latest_df = latest_df.fillna('')
            records = latest_df.to_dict('records')
        else:
            # Replace NaN with empty string for proper JSON serialization
            df = df.fillna('')
            records = df.to_dict('records')
        
        return jsonify({
            'success': True,
            'data': records
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/logs')
def get_logs():
    """Get recent log entries"""
    try:
        # Look for today's log file first
        today = datetime.now().strftime('%Y%m%d')
        log_path = f'./logs/weather_pipeline_{today}.log'
        
        # If today's log doesn't exist, try to find the most recent one
        if not os.path.exists(log_path):
            log_dir = './logs'
            if os.path.exists(log_dir):
                log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
                if log_files:
                    # Get the most recent log file
                    log_files.sort(reverse=True)
                    log_path = os.path.join(log_dir, log_files[0])
                else:
                    return jsonify({
                        'success': True,
                        'logs': []
                    })
            else:
                return jsonify({
                    'success': True,
                    'logs': []
                })
        
        # Read last 100 lines
        with open(log_path, 'r') as f:
            lines = f.readlines()
            recent_logs = lines[-100:] if len(lines) > 100 else lines
        
        return jsonify({
            'success': True,
            'logs': recent_logs
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error reading logs: {str(e)}'
        }), 500


@app.route('/api/stats')
def get_stats():
    """Get pipeline statistics"""
    try:
        csv_path = './data/weather_data.csv'
        
        if not os.path.exists(csv_path):
            return jsonify({
                'success': False,
                'message': 'No data available'
            }), 404
        
        df = pd.read_csv(csv_path)
        
        # Convert all numpy types to Python native types for JSON serialization
        stats = {
            'total_records': int(len(df)),
            'cities_count': int(df['city'].nunique()) if 'city' in df.columns else 0,
            'countries_count': int(df['country'].nunique()) if 'country' in df.columns else 0,
        }
        
        # Temperature stats
        if 'temperature' in df.columns:
            stats['temperature'] = {
                'avg': float(round(df['temperature'].mean(), 1)),
                'min': float(round(df['temperature'].min(), 1)),
                'max': float(round(df['temperature'].max(), 1))
            }
        
        # Humidity stats
        if 'humidity' in df.columns:
            stats['humidity'] = {
                'avg': float(round(df['humidity'].mean(), 1)),
                'min': float(round(df['humidity'].min(), 1)),
                'max': float(round(df['humidity'].max(), 1))
            }
        
        # Cities list
        if 'city' in df.columns:
            stats['cities'] = [str(city) for city in df['city'].unique().tolist()]
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/execute', methods=['POST'])
def execute_command():
    """Execute a Python command and return output"""
    try:
        data = request.get_json(silent=True) or {}
        command = data.get('command', '')
        
        # Whitelist of allowed commands for security
        allowed_commands = [
            'python3 view_data.py',
            'python3 examples/analyze_data.py',
            'python3 examples/test_components.py',
            'python3 examples/custom_pipeline.py',
            'python3 pipeline.py'
        ]
        
        if command not in allowed_commands:
            return jsonify({
                'success': False,
                'message': 'Command not allowed. Only whitelisted commands can be executed.'
            }), 403
        
        # Execute command with timeout
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            timeout=30,
            cwd=os.getcwd()
        )
        
        output = result.stdout if result.stdout else result.stderr
        
        return jsonify({
            'success': result.returncode == 0,
            'output': output,
            'exit_code': result.returncode
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'message': 'Command execution timeout (30 seconds)'
        }), 408
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("Weather Data Pipeline Dashboard")
    print("=" * 60)
    print("Starting web server on http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
