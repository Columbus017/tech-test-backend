import os
import json
import redis
import time
import logging
import pandas as pd
from datetime import datetime
from jsonschema import validate, exceptions
from email_validator import validate_email, EmailNotValidError

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Variables de Entorno ---
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
LOOKUP_FILE = 'data/lookup/departments.csv'
PROCESSED_DIR = 'data/processed_users'
DLQ_DIR = 'data/dlq'

# --- Esquema de Validación ---
USER_SCHEMA = {
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "firstName": {"type": "string", "minLength": 1},
    "email": {"type": "string", "format": "email"},
    "age": {"type": "number", "minimum": 10, "maximum": 65},
    "company": {
      "type": "object",
      "properties": {
        "department": {"type": "string", "minLength": 1}
      },
      "required": ["department"]
    }
  },
  "required": ["id", "firstName", "email", "age", "company"]
}

def load_department_lookup(filepath):
  try:
    df = pd.read_csv(filepath)
    return pd.Series(df.department_code.values, index=df.department_name).to_dict()
  except Exception as e:
    logging.error(f"Error loading lookup file {filepath}: {e}")
    return {}
  
def validate_record(record):
  try:
    # Validación de Esquema
    validate(instance=record, schema=USER_SCHEMA)
    # Validación de Email
    validate_email(record['email'], check_deliverability=False)

    return True, None
  
  except exceptions.ValidationError as e:
    return False, f"Schema error: {e.message}"
  except EmailNotValidError as e:
    return False, f"Invalid email: {str(e)}"
  except Exception as e:
    return False, f"Unexpected validation error: {str(e)}"
  
def process_file(raw_filepath, lookup_data):
  logging.info(f"Processing file: {raw_filepath}")

  os.makedirs(PROCESSED_DIR, exist_ok=True)
  os.makedirs(DLQ_DIR, exist_ok=True)
  timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
  processed_filename = os.path.join(PROCESSED_DIR, f'etl_{timestamp}.jsonl')
  dlq_filename = os.path.join(DLQ_DIR, f'invalid_users_{timestamp}.jsonl')
  
  valid_count = 0
  invalid_count = 0

  try:
    with open(raw_filepath, 'r', encoding='utf-8') as f_raw, \
         open(processed_filename, 'a', encoding='utf-8') as f_processed, \
         open(dlq_filename, 'a', encoding='utf-8') as f_dlq:
      
      for line in f_raw:
        try:
          record = json.loads(line)
          is_valid, error_reason = validate_record(record)

          if is_valid:
            dept_name = record['company']['department']
            record['department_code'] = lookup_data.get(dept_name, 'UNKNOWN')

            f_processed.write(json.dumps(record) + '\n')
            valid_count += 1
          else:
            record['error_reason'] = error_reason
            f_dlq.write(json.dumps(record) + '\n')
            invalid_count += 1

        except json.JSONDecodeError as e:
          logging.warning(f"Corrupted line in {raw_filepath}: {e}")
          f_dlq.write(json.dumps({"raw_line": line, "error_reason": "Invalid JSON"}) + '\n')
          invalid_count += 1
    
    logging.info(f"Process of {raw_filepath} complete. Valids: {valid_count}, invalids: {invalid_count}")
    return processed_filename, dlq_filename, valid_count, invalid_count

  except IOError as e:
    logging.error(f"I/O error processing {raw_filepath}: {e}")
    return None, None, 0, 0
  
def main():
  logging.info("Starting 'transformer' service...")

# --- Bucle de espera para Redis ---
  r = None
  while r is None:
    try:
      r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
      r.ping()
      logging.info("Connection with Redis set.")
    except redis.exceptions.ConnectionError as e:
      logging.warning(f"Cannot connect with Redis: {e}. Retry in 5s...")
      time.sleep(5)

  dept_lookup = load_department_lookup(LOOKUP_FILE)
  if not dept_lookup:
    logging.error("Can't load the lookup. Service cannot continue.")
    return
  
  while True:
    try:
      # r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
      p = r.pubsub()
      p.subscribe("channel:phase1_complete")
      logging.info("Subscribe to 'channel:phase1_complete'. Waiting messages")

      for message in p.listen():
        if message['type'] == 'message':
          data = json.loads(message['data'])
          raw_file = data['raw_file']

          if not os.path.exists(raw_file):
            logging.error(f"File not found: {raw_file}. Skiping...")
            continue

          processed_file, dlq_file, valid, invalid = process_file(raw_file, dept_lookup)

          if processed_file:
            message_data = json.dumps({
              "raw_file": raw_file,
              "processed_file": processed_file,
              "dlq_file": dlq_file,
              "valid_count": valid,
              "invalid_count": invalid
            })
            r.publish("channel:phase2_complete", message_data)
            logging.info(f"Message published on 'channel:phase2_complete'")
    
    except redis.exceptions.ConnectionError as e:
      logging.error(f"Connection error on Redis: {e}. Retry in 10s...")
      time.sleep(10)
    except Exception as e:
      logging.error(f"Unexpected error: {e}. Restarting in 10s...")
      time.sleep(10)

if __name__ == '__main__':
  main()