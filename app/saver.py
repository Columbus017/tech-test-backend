import os
import json
import redis
import time
import logging
import pandas as pd
import paramiko
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constantes ---
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Base de datos (Sqlite)
DB_PATH = '/app/database/data.db'
DB_ENGINE = create_engine(f'sqlite:///{DB_PATH}')

# SFTP config
SFTP_HOST = os.getenv('SFTP_HOST', 'sftp')
SFTP_PORT = int(os.getenv('SFTP_PORT', 22))
SFTP_USER = os.getenv('SFTP_USER', 'sftp_user')
SFTP_KEY_PATH = '/app/ssh_keys/id_rsa'
SFTP_REMOTE_DIR = '/upload'

FILE_TO_TABLE_MAP = {
  'raw_file': 'raw_users',
  'processed_file': 'processed_users',
  'dlq_file': 'invalid_users'
}

def save_to_database(filepaths):
  logging.info("Starting saving on database...")
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

  # --- Guardar el resumen de la run ---
  try:
    with DB_ENGINE.connect() as conn:
      with conn.begin():  # Iniciar transacción
        # Crear la tabla si no existe
        conn.execute(text("""
          CREATE TABLE IF NOT EXISTS etl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT,
            raw_file TEXT,
            processed_file TEXT,
            dlq_file TEXT,
            valid_count INTEGER,
            invalid_count INTEGER
          );
        """))
          
        # Insertar el registro de esta run
        conn.execute(text("""
          INSERT INTO etl_runs (run_timestamp, raw_file, processed_file, dlq_file, valid_count, invalid_count)
          VALUES (:ts, :rf, :pf, :df, :vc, :ic)
        """), {
          "ts": datetime.now().isoformat(),
          "rf": filepaths.get('raw_file'),
          "pf": filepaths.get('processed_file'),
          "df": filepaths.get('dlq_file'),
          "vc": filepaths.get('valid_count'),
          "ic": filepaths.get('invalid_count')
        })
    logging.info("'etl_runs' record saved successfully.")
  except Exception as e:
    logging.error(f"Critical error saving on 'etl_runs': {e}")
    logging.error(traceback.format_exc())
    return

  # --- Guardar cada archivo ---
  for key, table_name in FILE_TO_TABLE_MAP.items():
    filepath = filepaths.get(key)

    # --- Validación del archivo ---
    if not filepath:
        logging.warning(f"Key '{key}' not found on the message. Skipping.")
        continue
    if not os.path.exists(filepath):
        logging.warning(f"File not found: {filepath}. Skip saving on DB.")
        continue
    if os.path.getsize(filepath) == 0:
        logging.info(f"File empty: {filepath}. Skip saving on DB.")
        continue

    # --- Inicio de la transacción para este archivo ---
    try:
      logging.info(f"Loading {filepath} on table '{table_name}'...")
      
      # Leer el .jsonl
      df = pd.read_json(filepath, lines=True)

      # Aplanar tipos complejos
      for col in df.columns:
        if df[col].dtype == 'object':
          df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

      # Añadir columna de inserción
      df['insertion_date'] = datetime.now().isoformat()
      
      # Subir a Sqlite
      with DB_ENGINE.connect() as conn:
        with conn.begin():
          df.to_sql(table_name, con=conn, if_exists='append', index=False)
      
      logging.info(f"Table '{table_name}' saved successfully.")

    except Exception as e:
      logging.error(f"Error saving table '{table_name}' from {filepath}: {e}")
      logging.error(traceback.format_exc())

  logging.info("Database save cycle completed.")


def upload_to_sftp(filepaths):
  logging.info("Starting upload to SFTP...")
  
  sftp = None
  transport = None
  max_retries = 5
  
  for attempt in range(max_retries):
    try:
      private_key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
      transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
      transport.connect(username=SFTP_USER, pkey=private_key)
      sftp = paramiko.SFTPClient.from_transport(transport)
      logging.info(f"Connect to SFTP on {SFTP_HOST}:{SFTP_PORT} (Try {attempt+1})")
      break
    except Exception as e:
      logging.warning(f"Try {attempt+1}/{max_retries} failed. Cannot connect to SFTP: {e}. Retry in 5s...")
      if transport:
        transport.close()
      if attempt + 1 == max_retries:
        logging.error("All SFTP connection attempts failed. Aborting upload.")
        return
      time.sleep(5)
          
  if not sftp:
    logging.error("Unexpected fail, connection to SFTP is not active.")
    return

  try:
    files_to_upload = [
      filepaths.get('raw_file'),
      filepaths.get('processed_file'),
      filepaths.get('dlq_file')
    ]
    
    for local_path in files_to_upload:
      if not local_path or not os.path.exists(local_path):
        logging.warning(f"File not found or null: {local_path}. Skip upload to SFTP.")
        continue
          
      remote_filename = os.path.basename(local_path)
      remote_path = f"{SFTP_REMOTE_DIR}/{remote_filename}"
      logging.info(f"Uploading {local_path} a {remote_path}...")
      sftp.put(local_path, remote_path)
        
    logging.info("Upload to SFTP complete.")
      
  except Exception as e:
    logging.error(f"Error during SFTP upload (after connecting): {e}")
  finally:
    if sftp:
      sftp.close()
    if transport:
      transport.close()

def main():
  logging.info("Starting 'Saver' service...")
  time.sleep(5)

  r = None
  while r is None:
    try:
      r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
      r.ping()
      logging.info("Connection with Redis set.")
    except redis.exceptions.ConnectionError as e:
      logging.warning(f"Cannot connect with Redis: {e}. Retrying in 5s...")
      time.sleep(5)

  while True:
    try:
      p = r.pubsub()
      p.subscribe("channel:phase2_complete")
      logging.info("Suscribed to 'channel:phase2_complete'. Waiting messages...")

      for message in p.listen():
        if message['type'] == 'message':
          filepaths = json.loads(message['data'])
          logging.info(f"Message recieved. Start saving for: {filepaths.get('raw_file')}")

          save_to_database(filepaths)
          upload_to_sftp(filepaths)

          logging.info("Save cycle complete.")
    
    except redis.exceptions.ConnectionError as e:
      logging.error(f"Connection error on Redis: {e}. Retry in 10s...")
      time.sleep(10)
    except Exception as e:
      logging.error(f"Unexpected error: {e}. Restart in 10s...")
      time.sleep(10)

if __name__ == '__main__':
  main()