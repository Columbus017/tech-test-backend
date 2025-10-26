import os
import redis
import json
import time
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

API_BASE_URL = os.getenv('API_BASE_URL', 'https://dummyjson.com/users')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL_SECONDS', 60))
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
MAX_RETRIES = 3
STATE_FILE_PATH = 'data/state/extractor_state.json'
OUTPUT_DIR = 'data/raw_users'

def load_state():
  try:
    if os.path.exists(STATE_FILE_PATH):
      with open(STATE_FILE_PATH, 'r') as f:
        state = json.load(f)
        return state.get('last_skip', 0)
  except (IOError, json.JSONDecodeError) as e:
    logging.warning(f"Can't read the state file '{STATE_FILE_PATH}'. Beggining from zero. Error: {e}" )
  return 0

def save_state(skip_value):
  try:
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, 'w') as f:
      json.dump({'last_skip': skip_value}, f)
      logging.info(f"State saved: 'skip' set to {skip_value}")
  except IOError as e:
    logging.error(f"Critical error! Can't save the state in '{STATE_FILE_PATH}'. Error: {e}")

def fetch_batch(session, skip, limit):
  url = f"{API_BASE_URL}?limit={limit}&skip{skip}"

  for attempt in range(MAX_RETRIES):
    try:
      response = session.get(url, timeout=10)

      if response.status_code == 200:
        return response.json()
      else:
        logging.warnign(f"API return status {response.status_code}. Retry {attempt + 1}/{MAX_RETRIES}...")
    except requests.exceptions.RequestException as e:
      logging.warning(f"Exception in request: {e}. Retry {attempt + 1}/{MAX_RETRIES}...")

    if attempt < MAX_RETRIES - 1:
      wait_time = 5 * (3 ** attempt)
      time.sleep(wait_time)

  logging.error(f"Failed all {MAX_RETRIES} tries for the 'skip' {skip}.")
  return None

def run_extraction():
  logging.info("Starting new extraction cycle")

  current_skip = load_state()

  os.makedirs(OUTPUT_DIR, exist_ok=True)
  timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
  output_filename = os.path.join(OUTPUT_DIR, f'records_{timestamp}.jsonl')

  total_users = -1
  processed_count_in_run = 0

  with requests.Session() as session:
    while True:
      logging.info(f"Requesting batch: 'limit'={BATCH_SIZE}, 'skip'={current_skip}")

      data = fetch_batch(session, skip=current_skip, limit=BATCH_SIZE)

      if data is None:
        logging.error("Extraction failed. The state 'skip' wont change. Exit the process.")
        return False
    
      users = data.get('users', [])
      if total_users == -1:
        total_users = data.get('total', 0)
        logging.info(f"Total de usuarios a extraer: {total_users}")

      if not users:
        logging.info("No more users. Pagination completed")
        break

      try:
        with open(output_filename, 'a', encoding='utf-8') as f:
          for user in users:
            f.write(json.dumps(user) + '\n')
      except IOError as e:
        logging.error(f"Error writing in {output_filename}. Aborting. Error: {e}")
        return False
      
      current_skip += len(users)
      processed_count_in_run += len(users)

      save_state(current_skip)

      logging.info(f"Batch saved. Progress: {current_skip}/{total_users}")

      if current_skip >= total_users:
        logging.info(f"Extraction completed. Total users extracted this run: {processed_count_in_run}.")
        break

  logging.info("Extraction completed successfully. Reset 'skip' to 0 for next day.")
  save_state(0)

  if processed_count_in_run > 0:
    try:
      r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
      r.ping()

      message_data = json.dumps({"raw_file": output_filename})
      r.publish("channel:phase1_complete", message_data)

      logging.info(f"Message published on 'channel:phase1_complete': {output_filename}")
    except Exception as e:
      logging.error(f"Error publishing on Redis {e}")
  else:
    logging.info("No new records were processed, no publish on Redis.")

  return True

def main():
  while True:
    try:
      success = run_extraction()
      if success:
        logging.info(f"Extraction cycle success. Sleep for {SLEEP_INTERVAL} seconds...")
      else:
        logging.error(f"Extraction cycle fail. Retry in {SLEEP_INTERVAL} seconds...")

      time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
      logging.info("Manual stop recieved. Bye")
      break
    except Exception as e:
      logging.error(f"Unexpect error in main loop: {e}. Retry in 60s.")
      time.sleep(60)

if __name__ == "__main__":
  main()