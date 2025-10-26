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

DB_PATH = '/app/database/data.db'
DB_ENGINE = create_engine(f'sqlite:///{DB_PATH}')

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

# --- NUEVA FUNCIÓN (MÁS ROBUSTA) ---
def save_to_database(filepaths):
  """
  Guarda los datos en la base de datos en transacciones separadas
  para máxima robustez.
  """
  logging.info("Iniciando guardado en base de datos...")
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

  # --- PASO 1: Guardar el resumen de la corrida (Transacción Separada) ---
  try:
    with DB_ENGINE.connect() as conn:
      with conn.begin():  # Iniciar transacción
        # 1. Crear la tabla 'header' si no existe
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
          
        # 2. Insertar el registro de esta corrida
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
    logging.info("Registro de 'etl_runs' guardado exitosamente.")
  except Exception as e:
    logging.error(f"Error CRÍTICO guardando en 'etl_runs': {e}")
    logging.error(traceback.format_exc())
    # Si esto falla, no continuamos
    return

  # --- PASO 2: Guardar cada archivo (Transacciones Separadas) ---
  for key, table_name in FILE_TO_TABLE_MAP.items():
    filepath = filepaths.get(key)

    # --- Validación del archivo ---
    if not filepath:
        logging.warning(f"No se encontró la llave '{key}' en el mensaje. Saltando.")
        continue
    if not os.path.exists(filepath):
        logging.warning(f"Archivo no encontrado: {filepath}. Saltando guardado en DB.")
        continue
    if os.path.getsize(filepath) == 0:
        logging.info(f"Archivo está vacío: {filepath}. Saltando guardado en DB.")
        continue

    # --- Inicio de la transacción para ESTE archivo ---
    try:
      logging.info(f"Cargando {filepath} en tabla '{table_name}'...")
      
      # Leer el .jsonl (ahora es seguro)
      df = pd.read_json(filepath, lines=True)

      # Aplanar tipos complejos
      for col in df.columns:
        if df[col].dtype == 'object':
          df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

      # Añadir columna de inserción
      df['insertion_date'] = datetime.now().isoformat()
      
      # Cargar a Sqlite (en su propia transacción)
      with DB_ENGINE.connect() as conn:
        with conn.begin():
          df.to_sql(table_name, con=conn, if_exists='append', index=False)
      
      logging.info(f"Tabla '{table_name}' guardada exitosamente.")

    except Exception as e:
      logging.error(f"Error guardando la tabla '{table_name}' desde {filepath}: {e}")
      logging.error(traceback.format_exc())
      # Este error se registra, pero el bucle continuará con el siguiente archivo

  logging.info("Ciclo de guardado en base de datos completado.")


# --- (El resto de tu archivo: upload_to_sftp y main) ---
# ... (asegúrate de que el resto del archivo sigue ahí) ...

def upload_to_sftp(filepaths):
  logging.info("Iniciando subida a SFTP...")
  
  sftp = None
  transport = None
  max_retries = 5
  
  for attempt in range(max_retries):
    try:
      private_key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
      transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
      transport.connect(username=SFTP_USER, pkey=private_key)
      sftp = paramiko.SFTPClient.from_transport(transport)
      logging.info(f"Conectado a SFTP en {SFTP_HOST}:{SFTP_PORT} (Intento {attempt+1})")
      break
    except Exception as e:
      logging.warning(f"Intento {attempt+1}/{max_retries} fallido. No se pudo conectar a SFTP: {e}. Reintentando en 5s...")
      if transport:
        transport.close()
      if attempt + 1 == max_retries:
        logging.error("Todos los intentos de conexión SFTP fallaron. Abortando subida.")
        return
      time.sleep(5)
          
  if not sftp:
    logging.error("Fallo inesperado, la conexión SFTP no está activa.")
    return

  try:
    files_to_upload = [
      filepaths.get('raw_file'),
      filepaths.get('processed_file'),
      filepaths.get('dlq_file')
    ]
    
    for local_path in files_to_upload:
      if not local_path or not os.path.exists(local_path):
        logging.warning(f"Archivo no encontrado o nulo: {local_path}. Saltando subida a SFTP.")
        continue
          
      remote_filename = os.path.basename(local_path)
      remote_path = f"{SFTP_REMOTE_DIR}/{remote_filename}"
      logging.info(f"Subiendo {local_path} a {remote_path}...")
      sftp.put(local_path, remote_path)
        
    logging.info("Subida a SFTP completada.")
      
  except Exception as e:
    logging.error(f"Error durante la subida a SFTP (después de conectar): {e}")
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
      logging.info("Conexión con Redis establecida.")
    except redis.exceptions.ConnectionError as e:
      logging.warning(f"No se puede conectar a Redis: {e}. Reintentando en 5s...")
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