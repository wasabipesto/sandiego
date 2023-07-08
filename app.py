from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import requests
import numpy as np

def main():
    print('App started.')
    

if __name__ == "__main__":
    scheduler.add_job(
        main, 'interval', minutes=60, 
        start_date=(datetime.now()+timedelta(seconds=5))
        )
    scheduler.start()