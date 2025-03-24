from audio_worker import connect_news_server,  translate_generate_audio, send_rtmp_audio, formulate_update_twitter, formulate_update_audio, select_calendar_events_for_audio

from get_token import get_token
import asyncio
import requests
import time
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import logging 
env_path = Path(os.getcwd()) / '.env'
load_dotenv(dotenv_path=env_path)


NEWS= asyncio.Queue()
AUDIO_STREAM = bytearray()
CALENDAR_UPDATE= None  
SWISSCALENDAR_API_URL= os.getenv("SWISSCALENDAR_API_URL")
WORKER_EMAIL=os.getenv("WORKER_EMAIL")
WORKER_PASSWORD=os.getenv("WORKER_PASSWORD")

logging.basicConfig(
    filename="logfile.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__) 


async def run_audio(NEWS,AUDIO_STREAM):
          
        await asyncio.gather(
        add_calendar_update(CALENDAR_UPDATE, NEWS),
        connect_news_server(NEWS),
        translate_generate_audio(NEWS,AUDIO_STREAM),
        send_rtmp_audio(AUDIO_STREAM)
        )
    

async def add_calendar_update(update, news):
    """Polls the specified API endpoint and processes updates."""

    
    interval = 55
    global CALENDAR_UPDATE  # Consider if this really needs to be global
    
    #token=get_token(WORKER_EMAIL,WORKER_PASSWORD)
    #swisscalendar_api_url = str(SWISSCALENDAR_API_URL) + "/api/updatetoday"
    #header= {'Content-Type': 'application/json','token': f'{token}'}
    
    while True:
        data= {}
        try:
            events = select_calendar_events_for_audio()
            
            for data in events:
                if data:
                        logger.info(f"Calendar update data received: {data}")  # Log the data
                        CALENDAR_UPDATE = await formulate_update_audio(data)
                        if CALENDAR_UPDATE:
                            logger.info("Calendar update added for twitter and audio")  # More descriptive messag
                            await Add_CalendarUpdate_News(CALENDAR_UPDATE, news)
                            update_twitter = await formulate_update_twitter(data)  # Assuming this function exists
                        else:
                            logger.warning(f"No audio generated, tweet sent. AI response is {CALENDAR_UPDATE}")  # More descriptive message
                else:
                        logger.info("No update received from API (empty data).")  # More descriptive message
                

        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to API: {e}")
            logger.warning(f"{response.text}")
        except Exception as e:  # Catch other potential errors
            logger.exception(f"An unexpected error occurred: {e}")  # Log the full traceback
        
        if (data=={}):
            await asyncio.sleep(interval)
             
        else:
            await asyncio.sleep(5)
    



async def Add_CalendarUpdate_News(update: str, news: asyncio.Queue):
    """Adds an update to the front of the news queue (asyncio-safe)."""

    async with asyncio.Lock():  # Use asyncio.Lock!
        temp_queue = asyncio.Queue()

        await temp_queue.put(update)

        while not news.empty():
            item = news.get_nowait()
            await temp_queue.put(item)

        while not temp_queue.empty():
            item = temp_queue.get_nowait()
            await news.put(item)


asyncio.run(run_audio(NEWS,AUDIO_STREAM))
#asyncio.run(add_calendar_update(CALENDAR_UPDATE,NEWS))   