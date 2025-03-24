import json
import boto3
import os
import requests
import time
import logging
from langfuse.decorators import observe, langfuse_context
from langfuse.openai import openai
from langfuse import Langfuse
import httpx
import asyncio
import subprocess
import aiofiles
import requests
import websockets
import random
from requests_oauthlib import OAuth1
import ast
import tempfile
import datetime





#GLOBAL VARIABLES TO STORE TEXT AND AUDIO 
NEWS= asyncio.Queue()
AUDIO_STREAM = bytearray()
event_list = []
current_day= ""

# Set up logging
logger = logging.getLogger(__name__) 

# Retrieve environment variables
from pathlib import Path  
from dotenv import load_dotenv
env_path = Path(os.getcwd()) / '.env'
load_dotenv(dotenv_path=env_path)

SERVER = os.getenv("SERVER")
OPENAI_API_KEY=os.getenv("OPEN_AI_API")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY")
LANGFUSE_PUBLIC_KEY =os.getenv("LANGFUSE_PUBLIC_KEY")
LANGFUSE_HOST = os.getenv("LANGFUSE_HOST")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
VOICES = ast.literal_eval(os.getenv("VOICES"))
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
voice_id = os.getenv("voice_id")
openai_api_url = os.getenv("openai_api_url")
prompt_translation_no_tts=os.getenv("prompt_translation_no_tts")
model_translation_no_tts=os.getenv("model_translation_no_tts")
LIVESQUAWK_PARTNER_APIKEY= os.getenv("LIVESQUAWK_PARTNER_APIKEY")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET_KEY = os.getenv("TWITTER_API_SECRET_KEY")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
RTMP_URL = "rtmps://" + str(os.getenv("WOWZA_URL"))
CALENDAR_URL=os.getenv("CALENDAR_URL")
CALENDAR_API_KEY=os.getenv("CALENDAR_API_KEY")

#INITIATE OBJECTS AND REQUEST PARAMS
s3_client = boto3.client('s3')
elevenlabs_request_headers = {
        "Accept": "application/json",
        "xi-api-key": ELEVENLABS_API_KEY,
        "model":"gpt-4o"
    }
voice_synthesis_config = {
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.8,
            "style": 0.0,
            "use_speaker_boost": True
        },
        "output_format": "mp3_44100_64"
    }
openai_request_headers = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json"
}

langfuse = Langfuse(  # Langfuse client for tracing
    secret_key=LANGFUSE_SECRET_KEY, 
    public_key=LANGFUSE_PUBLIC_KEY, 
    host=LANGFUSE_HOST
)

# INITIATE VARIABLES
chat_context = []  # (Currently unused) Could store conversation history
translation_priority = True  # Flag to indicate if translation should be prioritized
translated_response = ""  # Stores the translated text output
translation_results = []  # List to store translation, priority, and entity results
recognized_entities = []  # List to store recognized entities from the text
final_output_text = ""  # Final processed text to be used for TTS
audio_chunk_sent_time = 0  # Timestamp when the first audio chunk is sent
server_start_time = time.time()  # Record the server's start time


#CONSTANTS
CHUNK_SIZE=10240
SELECTED_SERVER="3010"
WEBSOCKET_URL_BASE="wss://ws.livesquawk.com/ws"




async def translate_generate_audio(news,audio_stream):    
    tts="True"
    while True:
        text= await news.get()
        text_output=""
        if text is not None:
            logger.debug(f"Translation started:  {text}")
            index = random.randint(0,2)
            voice_id = VOICES[index]
            logger.debug(f"voice_id is {voice_id} in {VOICES}")
            # Perform translation
            translation_start_time = time.time()
            try:
                await translate_work(text,True, translation_results)
                translation_end_time = time.time()
                text_output = post_process_text(text, *translation_results)  # Apply post-processing
                
                logger.debug(f"Translation ended: {text_output}")
                # Initialize variables for audio streaming
                
                sending_first_chunk = True
                s3_object_key = ""  # Generate a unique key for S3 storage
                first_chunk_sent_time = 0
                last_chunk_sent_time = 0
            
            except Exception as e:
                logger.warning(f"Error streaming audio: {str(e)}")
            
            # Generate and stream audio if TTS is requested
            try:
                first_chunk_sent_time, last_chunk_sent_time, s3_object_key= await generate_ai_speech([text_output], voice_id, audio_stream)
            
            except Exception as e:
                logger.warning(f"Error streaming audio: {str(e)}")
            logger.info(f" original {text}, translation {text_output}")
            logger.debug(f" original {text}, translation {text_output}, translation start time {int(translation_start_time)} translation end time {int(translation_end_time)}, first audio chunk {int(first_chunk_sent_time)}, last audio chunk {int(last_chunk_sent_time)} , file {s3_object_key}, voice_id  {voice_id}")
            langfuse_context.flush()  # Flush Langfuse context for tracing



async def request_server_assignment(selected_server):    
    initial_server=3010
    async with websockets.connect(WEBSOCKET_URL_BASE+str(initial_server)) as websocket:
        logger.info(" request to assign server")
        
        await asyncio.wait_for(websocket.send (json.dumps({"type": "request_server_assignment"})), timeout=10)
        try:
            msg = await asyncio.wait_for(websocket.recv(), timeout=10)
            selected_server=str(json.loads(msg)["selected_server"])
        except asyncio.TimeoutError:      
                    logger.warning(f"Timeout reached. Retrying connection")
                    await asyncio.sleep(10)
                    await request_server_assignment()
            
        except websockets.ConnectionClosed:
                    logger.error("Fatal error, audio generation function exit")
        return selected_server

import asyncio
import websockets
import json
import logging

# ... (other imports and constants)

logger = logging.getLogger(__name__)  # Initialize logger

async def connect_news_server(news):
    global SELECTED_SERVER
    while True:  # Outer loop for reconnection attempts
        try:
            SELECTED_SERVER = await request_server_assignment(SELECTED_SERVER)
            logger.info(f"Connecting to {SELECTED_SERVER}...")  # Use logger
           
            async with websockets.connect(WEBSOCKET_URL_BASE + SELECTED_SERVER) as websocket:
                logger.info("Connection to assigned server established.")  # Use logger

                try:
                    logger.debug("inside outer loop")
                    await asyncio.wait_for(
                        websocket.send(
                            json.dumps(
                                {
                                    "type": "join_publisher_channel",
                                    "api_key": LIVESQUAWK_PARTNER_APIKEY,
                                }
                            )
                        ),
                        timeout=10,
                    )
                    logger.debug("connected")
                except asyncio.TimeoutError:
                    logger.error("Timeout sending join message. Disconnecting.")
                    break  # Break out of the inner (websocket) loop to reconnect

                while True:  # Inner loop for message processing
                    
                    try:
                        msg = await asyncio.wait_for(websocket.recv(), timeout=40)
                        message = json.loads(msg)
                        logger.debug(f"Received message: {message}")  # Log at debug level

                        if message.get("type") == "new_note":  # Use .get() to avoid KeyError
                            await news.put(message["title"])
                    except asyncio.TimeoutError:
                        logger.warning("No packets received. Checking connection...")
                        try:
                            await websocket.ping()  # Check if the connection is still alive
                            logger.debug("Websocket connection is still alive.")
                        except websockets.ConnectionClosed:
                            logger.warning("Websocket connection is closed. Reconnecting...")
                            break  # Break inner loop to reconnect
                        except Exception as e:
                            logger.error(f"Error during ping: {e}")
                            break  # Break inner loop to reconnect
                    except websockets.ConnectionClosed as e:  # Include the exception in the log
                        logger.warning(f"WebSocket connection closed: {e}. Reconnecting...")
                        break  # Break inner loop to reconnect
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON: {e}. Message: {msg}")  # Log the raw message
                    except Exception as e:
                        logger.exception(f"An unexpected error occurred: {e}")  # Log full traceback

        except Exception as e:  # Catch exceptions during connection
            logger.error(f"Error during connection attempt: {e}. Retrying in 20 seconds...")
            await asyncio.sleep(20)  # Wait before retrying connection

    logger.info("connect_news_server finished") # Log message to indicate that the function has finished



async def generate_ai_speech(translated_text, voice_id, buffer):
    """
    Converts translated text to speech using ElevenLabs API and streams 
    the audio chunks to a shared asyncio.Queue.
    """

    elevenlabs_tts_url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
        
    logger.debug(f"starting TTS for {translated_text}")
    for text in translated_text:   
        logger.info(f"generating voice for text: {text}")
        tts_request_payload = {**voice_synthesis_config, "text":text}
        try:
                time_start_audio=time.time() 
                response = requests.post(
                    elevenlabs_tts_url, 
                    headers=elevenlabs_request_headers, 
                    json=tts_request_payload, 
                    stream=True, 
                    timeout=30
                )
                
                #response.raise_for_status()
                with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp_file:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        buffer.extend(chunk)
                        tmp_file.write(chunk)
        except Exception as e:
                    logger.error(f"Error generating audio: {e}")

        '''
        except (boto3.exceptions.Boto3Error, Exception) as e:
                    logger.error(f"Error uploading speech to S3: {e}")


        
        
        # Upload the generated audio to S3
        
        try:
            with open(temp_audio_filepath, "rb") as f:
                    audio_data = f.read()
                    s3_client.put_object(
                        Bucket=S3_BUCKET_NAME, 
                        Key=temp_audio_filepath, 
                        Body=audio_data, 
                        ContentType='audio/wav'
                    )
                      
                
        except Exception as e:
                logger.error(f"Unexpected error in to_speech: {e}")
        
        os.remove(temp_audio_filepath)
        '''
        
    return  time_start_audio, time.time() , "a" #temp_audio_filepath



async def send_rtmp_audio(buffer):    
    ffmpeg_cmd = [
    "ffmpeg",
    "-re",
    "-i", "-",
    "-vn",
    "-c:a", "aac",
    "-ar", "48000",
    "-b:a", "64k",
    "-f", "flv",
    RTMP_URL ]

    #Silent MP3 from a file to fill gaps when the buffer starves
    with open("audio/mp3/b.mp3", "rb") as f:
        silent_mp3 = f.read()
        silent_mp3_length=len(silent_mp3)
        starting_point_mp3=0
    START_TIME=time.time()

    
    with subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE) as process:
        logger.info("launched ffmpeg")
        
        try:
            while True:
                
                while len(buffer)>0:
                        logger.debug(f"{(time.time()-START_TIME):.2f}: buffer size: {len(buffer)}")
                        process.stdin.write(buffer[:CHUNK_SIZE])
                        del buffer[:CHUNK_SIZE]
                        await asyncio.sleep(0.3)
                
                #logger.info(f"{(time.time()-START_TIME):.2f}: empty")
                audio_input=silent_mp3[starting_point_mp3:starting_point_mp3+CHUNK_SIZE]
                
                if len(audio_input)>CHUNK_SIZE-2:
                    process.stdin.write(audio_input)
                    starting_point_mp3= starting_point_mp3+CHUNK_SIZE
                
                else:
                    starting_point_mp3=0
                     
                await asyncio.sleep(0.3)
        except BrokenPipeError as e:
            logger.warning("FFmpeg process closed unexpectedly:", e) 
        except Exception as e:
            logger.warning("An error occurred:", e)
            process.kill()
        
        finally:
            process.stdin.close()
            process.wait()





# --------------------- Main Translation Function --------------------- 

@observe(as_type="generation")
async def translate(text,tts):
    """
    Translates text using the OpenAI API.
    """
    global translated_response
    if tts=="True":
        prompt_entities = langfuse.get_prompt(prompt_translation_tts)
        model_translation=model_translation_tts
    else:
        prompt_entities = langfuse.get_prompt(prompt_translation_no_tts)
        model_translation=model_translation_no_tts
    
    prompt = prompt_entities.compile(text=text)
    
    payload = {"model": model_translation, "messages": prompt, "max_tokens": 200}

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        response = await client.post(openai_api_url, headers=openai_request_headers, json=payload)

    if response.status_code == 200:
        translated_response = response.json()["choices"][0]["message"]["content"].strip()
        logger.info(f"Translated text: {translated_response}")
        langfuse_context.update_current_trace(metadata={"input":text, "output": translated_response,"tts":tts})
        return translated_response
    else:
        logger.warning(f"Error translating: {text}")
        response.raise_for_status()
        return f"Error: {response.status_code} - {response.text}"

# ----------------------- Helper Functions ----------------------- 


def post_process_text(text, translated_response, translation_priority, recognized_entities):
    """
    (Placeholder) Post-processes the translated text.
    """
    result = ""
    if translation_priority != False:
        result = translated_response
    return result

async def translate_work(text,tts, results):
    """
    (Placeholder) Manages the translation workflow.
    """
    global translation_results
    translation_results = await asyncio.gather(
        translate(text,tts), assess_priority(text), recognize_entities(text)
    )
    return translation_results


#@observe(as_type="generation")
async def recognize_entities(text):
    """
    (Placeholder) Recognizes entities in the text.
    """
    pass

#@observe(as_type="generation")
async def assess_priority(text):
    """
    (Placeholder) Assesses the priority of the text.
    """
    pass


#--------------transform a calendar update to news ------------#


Template= [
  {"zone": "US", "indicator": "Créations d'emplois dans le secteur non agricole (NFP)", "periodicity": "mensuel"},
  {"zone": "US", "indicator": "Créations d’emplois non agricole ADP", "periodicity": "mensuel"},
  {"zone": "US", "indicator": "Inscriptions au chômage", "periodicity": "hebdomadaire"},
  {"zone": "US", "indicator": "EIA - Stock pétrole brut", "periodicity": "hebdomadaire"},
  {"zone": "US", "indicator": "PCE core", "periodicity": "mensuel"},
  {"zone": "US", "indicator": "PCE core", "periodicity": "annuel"},
  {"zone": "US", "indicator": "PCE core préliminaire", "periodicity": "trimestriel"},
  {"zone": "US", "indicator": "Conference Board - Confiance des consommateurs", "periodicity": "mensuel"},
  {"zone": "US", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "US", "indicator": "FED - Décision sur les taux d'intérêt", "periodicity": "ponctuel"},
  {"zone": "US", "indicator": "PIB", "periodicity": "trimestriel"},
  {"zone": "EU", "indicator": "PIB", "periodicity": "annuel"},
  {"zone": "EU", "indicator": "PIB", "periodicity": "trimestriel"},
  {"zone": "EU", "indicator": "S&P Global / HCOB - PMI services", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "S&P Global / HCOB - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "S&P Global / HCOB - PMI composite", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "S&P Global / HCOB - PMI construction", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "ZEW - Sentiment économique", "periodicity": "mensuel"},
  {"zone": "EU", "indicator": "BCE - Décision sur les taux d'intérêt", "periodicity": "ponctuel"},
  {"zone": "UK", "indicator": "S&P Global / CIPS - PMI services", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "S&P Global / CIPS - PMI composite", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "S&P Global / CIPS - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "S&P Global / CIPS - PMI construction", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "PIB", "periodicity": "annuel"},
  {"zone": "UK", "indicator": "PIB", "periodicity": "mensuel"},
  {"zone": "UK", "indicator": "PIB", "periodicity": "trimestriel"},
  {"zone": "UK", "indicator": "BoE - Décision sur les taux d’intérêt", "periodicity": "ponctuel"},
  {"zone": "FR", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global / HCOB - PMI services", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global / HCOB - PMI composite", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global / HCOB - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global / HCOB - PMI construction", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "PIB", "periodicity": "annuel"},
  {"zone": "FR", "indicator": "PIB", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global - PMI composite", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global - PMI services", "periodicity": "mensuel"},
  {"zone": "FR", "indicator": "S&P Global - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "S&P Global / HCOB - PMI services", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "S&P Global / HCOB - PMI composite", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "S&P Global / HCOB - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "S&P Global / HCOB - PMI construction", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "IFO - Perspectives des entreprises", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "IFO - Situation actuelle", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "IFO - Climat des affaires", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "PIB", "periodicity": "trimestriel"},
  {"zone": "DE", "indicator": "PIB", "periodicity": "annuel"},
  {"zone": "DE", "indicator": "ZEW - Sentiment économique", "periodicity": "mensuel"},
  {"zone": "DE", "indicator": "ZEW - Situation actuelle", "periodicity": "mensuel"},
  {"zone": "CH", "indicator": "Procure.ch / SVME - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "CH", "indicator": "PIB", "periodicity": "trimestriel"},
  {"zone": "CH", "indicator": "PIB", "periodicity": "annuel"},
  {"zone": "CH", "indicator": "ZEW - Indice des perspectives conjoncturelles", "periodicity": "mensuel"},
  {"zone": "CH", "indicator": "Balance commerciale (CHF)", "periodicity": "mensuel"},
  {"zone": "CA", "indicator": "S&P Global - PMI manufacturier", "periodicity": "mensuel"},
  {"zone": "CA", "indicator": "Balance commerciale", "periodicity": "mensuel"},
  {"zone": "CA", "indicator": "PIB", "periodicity": "mensuel"},
  {"zone": "CA", "indicator": " Décision de la BoC sur les taux d’intérêt", "periodicity": "ponctuel"}

]

all_event_template="""
USA - Créations d'emplois dans le secteur non agricole (NFP)
USA – Créations d’emplois non agricole ADP 
USA - Inscriptions au chômage (Hebdomadaire)
USA - EIA - Stock pétrole brut (Hebdomadaire)
USA - PCE core (Mensuel)
USA - PCE core (Annuel) 
USA - PCE core (Trimestriel) préliminaire 
USA - Conference Board - Confiance des consommateurs novembre 
USA - Balance commerciale
USA - FED - Décision de la FED sur les taux d'intérêt
USA - PIB (Trimestriel) 
Zone Euro - PIB (Annuel) 
Zone Euro - PIB (Trimestriel) 
Zone-Euro - S&P Global / HCOB - PMI services
Zone-Euro - S&P Global / HCOB - PMI manufacturier
Zone-Euro - S&P Global / HCOB - PMI composite
Zone-Euro  - S&P Global / HCOB - PMI construction
Zone-Euro - Balance commerciale
Zone-Euro - ZEW - Sentiment économique
Zone Euro - PIB (Trimestriel)
Zone Euro - BCE - Décision sur les taux d'intérêt
Royaume-Uni - S&P Global / CIPS - PMI services 
Royaume-Uni - S&P Global / CIPS - PMI composite
Royaume-Uni - S&P Global / CIPS - PMI manufacturier 
Royaume-Uni - S&P Global / CIPS - PMI construction
Royaume-Uni - Balance commerciale septembre 
Royaume-Uni - PIB (Annuel) septembre 
Royaume-Uni - PIB (Mensuel) septembre
Royaume-Uni - PIB (Annuel)  
Royaume-Uni – BoE - Décision sur les taux d’intérêt
France - Balance commerciale
France - S&P Global / HCOB - PMI services
France - S&P Global / HCOB - PMI composite
France - S&P Global / HCOB - PMI manufacturier
France - S&P Global / HCOB - PMI construction
France - PIB (Annuel) 
France - PIB (Mensuel) 
France - PIB (Annuel)
France - S&P Global - PMI composite
France - S&P Global - PMI services
France - S&P Global - PMI manufacturier
Allemagne - S&P Global / HCOB - PMI services
Allemagne - S&P Global / HCOB - PMI composite
Allemagne - S&P Global / HCOB - PMI manufacturier
Allemagne - S&P Global / HCOB - PMI construction
Allemagne - Balance commerciale
Allemagne - IFO - Perspectives des entreprises novembre
Allemagne - IFO - Situation actuelle novembre 
Allemagne - IFO - Climat des affaires novembre
Allemagne - PIB TrimestrielPréliminaire
Allemagne - PIB Annuel)
Allemagne - ZEW - Sentiment économique novembre 
Allemagne - ZEW - Situation actuelle novembre
Suisse - Procure .ch / SVME - PMI manufacturier
Suisse - PIB (Trimestriel) 
Suisse - PIB (Annuel)
Suisse - ZEW - Indice des perspectives conjoncturelles 
Suisse - Balance commerciale (CHF) 
Canada - S&P Global - PMI manufacturier
Canada - Balance commerciale
Canada - PIB (Mensuel) octobre"""


def compile_template(country):
    result=""
    for event in Template:
        if event["zone"]==country:
            result= f" {result} {event['indicator']} -- ({event['periodicity']}) \n \n"
    return result
            

@observe(as_type="generation")
async def formulate_update_audio(row):
    """
    This function processes the received record (JSON object) and performs
    necessary actions with the update.  You'll need to implement the
    specific logic here based on your requirements.

    Args:
        row: A dictionary representing the JSON data from the API.
    """

    try:
        record={
            "actual": row["actual"],
            "country":  row["country"],
            "event":  row["event"],
            "forecast":  row["forecast"],
            "period": row["period"],
        }

        prompt_check_template = langfuse.get_prompt("check_record_template")

        if record["country"]:    
            Template_compiled= compile_template(record["country"])
        else: 
            Template_compiled== all_event_template
            logger.warning("skipping audio for calendar update - no country") 
            

        if  Template_compiled=="" or len(row["actual"])==0 :
            logger.warning(f"country {row['country']} not covered or actual ({row['actual']}) is not announced yet")
            return None
            
        logger.info(f"checking if -- {row['event_french']} -- is in template \n \n {Template_compiled}") 
        
        prompt = prompt_check_template.compile(message=record, template=Template_compiled)
        payload = {"messages": prompt, "model":"gpt-4o", "max_tokens": 200}
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            response = await client.post(openai_api_url, headers=openai_request_headers, json=payload)

        logger.info(f"response is {str(response.json())}")
        if response.status_code == 200:
            update = ast.literal_eval(response.json()["choices"][0]["message"]["content"].strip())
            logger.info(f" Update worth saying : {update}")
            langfuse_context.update_current_trace(metadata={"input":record, "output": update})    
            
            if (update['indicateur_clef']==1) :
                return update["reformulation"]
            else:
                logger.warning(f"skipping audio for calendar update - no indicateur clef - response {update}")
                return None 
            
        else:
            logger.warning(f"Reformulation OPENAI call not working {update}")    
            return None 
            
    except Exception as e:
        logger.warning(f"Error processing update: {e}")


observe(as_type="generation")
async def formulate_update_twitter(row):
    """
    This function processes the received record (JSON object) and performs
    necessary actions with the update.  You'll need to implement the
    specific logic here based on your requirements.

    Args:
        row: A dictionary representing the JSON data from the API.
    """

    try:
        record={
            "actual": row["actual"],
            "country":  row["country"],
            "event":  row["event"],
            "forecast":  row["forecast"],
            "period": row["period"],
        }

        prompt_check_template = langfuse.get_prompt("check_record_template_twitter")

        if record["country"]:
            Template_compiled= compile_template(record["country"])

        else: 
            Template_compiled== all_event_template 
        if  Template_compiled=="":
            return None
            
        
        prompt = prompt_check_template.compile(message=record, template=Template_compiled)
        payload = {"messages": prompt, "model":"gpt-4o", "max_tokens": 200}
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            response = await client.post(openai_api_url, headers=openai_request_headers, json=payload)
            
        if response.status_code == 200:
            update = ast.literal_eval(response.json()["choices"][0]["message"]["content"].strip())
            logger.info(f"Updates: {update}")
            langfuse_context.update_current_trace(metadata={"input":record, "output": update})         
            try:
                send_to_x(update["reformulation"]) 
            except Exception as e:
                logger.warning(f"Error tweeting: {str(e)}")
            return update["reformulation"]
            
        else:
                return None 
            
    except Exception as e:
        logger.warning(f"Error processing update: {e}")


def send_to_x(tweet_text):
    global TWITTER_API_KEY,TWITTER_API_SECRET_KEY,TWITTER_ACCESS_TOKEN,TWITTER_ACCESS_TOKEN_SECRET
    # Authenticate with OAuth 1.0a
    auth = OAuth1(TWITTER_API_KEY, TWITTER_API_SECRET_KEY, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

    # Twitter API Endpoint for Posting Tweets
    url = "https://api.twitter.com/2/tweets"


    # Payload (Twitter API v2 requires JSON format)
    payload = {
        "text": tweet_text
    }

    # Send Request to Post Tweet
    response = requests.post(url, json=payload, auth=auth)
    # Check Response
    if response.status_code == 201:
        logger.info("Tweet posted successfully:", response.json())
    else:
        logger.warning("Tweet Error:", response.status_code, response.text)

"""
buffer = bytearray()

async def main():
    voice_id="TQaDhGYcKI0vrQueAmVO"
    print(len(buffer))
    await generate_ai_speech(["hello"], voice_id, buffer)
    print(len(buffer))

asyncio.run(main()) 
"""


def select_todays_events_from_api():
    global CALENDAR_API_KEY
    url = CALENDAR_URL
    headers = {
        'accept': 'application/json',
        'API_KEY': CALENDAR_API_KEY

    }

    response = requests.get(url, headers=headers)

    data = {'events':[]}
    if response.status_code == 200:
        data = response.json()
        
    else:
        logger.error(f"Failed to fetch data. Status code: {response.status_code}")
    return data



def select_calendar_events_for_audio():
    global event_list
    global current_day
    selected_events=[]

    current_hour = datetime.datetime.now(datetime.timezone.utc).hour

    events=select_todays_events_from_api()["events"]
    if events is None:
        logger.warning("No events returned from Calendar API today's events. Skipping the rest")
        return []
    
    for event in events:
        logger.debug(event)
        try: 
        
            time_str=event["time"]
            time_obj = datetime.datetime.strptime(time_str, "%H:%M")
            dict_hour = time_obj.hour
            
            
            if (event["actual"]!="") and (event not in event_list) :
                event_list.append(event)
                if (current_hour-dict_hour)>=0 and (current_hour-dict_hour)<3:
                    selected_events.append(event)
                
            
            if event["date"]!=current_day:
                current_day= event["date"]
                event_list=[]

        except Exception as e: 
            logger.warning("error selecting events from calendar for audio")
            
    logger.debug(f" List of Events previously processed  {[event['event'] for event in event_list]}")
    logger.info(f"\n Events to be processed now  {[event['event'] for event in selected_events]}") 
              
    return selected_events