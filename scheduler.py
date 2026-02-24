"""
Scheduled Pipeline Runner
Runs the weather data pipeline at regular intervals
"""
import os
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv
from pipeline import WeatherPipeline


def run_pipeline_job():
    """Job to run the pipeline"""
    print(f"\n{'='*60}")
    print(f"Running scheduled pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    try:
        pipeline = WeatherPipeline()
        success = pipeline.run()
        
        if success:
            print(f"\n✅ Pipeline completed successfully at {datetime.now().strftime('%H:%M:%S')}")
        else:
            print(f"\n❌ Pipeline failed at {datetime.now().strftime('%H:%M:%S')}")
    
    except Exception as e:
        print(f"\n❌ Error running pipeline: {str(e)}")


def main():
    """Main scheduler function"""
    load_dotenv()
    
    # Get interval from environment (default: 60 minutes)
    interval = int(os.getenv('FETCH_INTERVAL_MINUTES', 60))
    
    print("""
    ╔══════════════════════════════════════════════════════╗
    ║       Weather Data Pipeline - Scheduler Mode         ║
    ║              Automated Data Collection               ║
    ╚══════════════════════════════════════════════════════╝
    """)
    
    print(f"📅 Schedule: Every {interval} minutes")
    print(f"⏰ Next run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Press Ctrl+C to stop\n")
    
    # Schedule the job
    schedule.every(interval).minutes.do(run_pipeline_job)
    
    # Run immediately on start
    run_pipeline_job()
    
    # Keep running scheduled jobs
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        print("\n\n🛑 Scheduler stopped by user")


if __name__ == '__main__':
    main()
