import os
import json
import redis
import time
import logging
import pandas as pd
import paramiko
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

DB_PATH = 'database/data.db'
DB_ENGINE = create_engine(f'sqlite:///{DB_PATH}')

SFTP_HOST = os.getenv('SFTP_HOST', 'sftp')
SFTP_PORT = int(os.getenv('SFTP_PORT', 22))
SFTP_USER = os.getenv('SFTP_USER', 'sftp_user')
SFTP_KEY_PATH = '/app/ssh_keys/id_rsa'
SFTP_REMOTE_DIR = '/upload'

# ... (imports, incluyendo 'import json') ...

def save_to_database(filepaths):
  """
  Lee los archivos .jsonl y los guarda en tablas Sqlite.
  Cumple con: Requisito de Base de Datos
  """
  logging.info("Guardando archivos en la base de datos...")
  
  # ... (asegurar que la carpeta DB exista) ...
  
  file_to_table_map = {
    'raw_file': 'raw_users',
    'processed_file': 'processed_users',
    'dlq_file': 'invalid_users'
  }
    
  try:
    with DB_ENGINE.connect() as conn:
      # ... (código de crear/insertar en 'etl_runs') ...

      # Procesar cada uno de los 3 archivos
      for key, table_name in file_to_table_map.items():
        filepath = filepaths[key]
        # ... (check si el archivo existe) ...
            
        logging.info(f"Cargando {filepath} en tabla '{table_name}'...")
        
        # Leer el .jsonl
        df = pd.read_json(filepath, lines=True)
        
        # --- NUEVO CÓDIGO: Aplanar tipos complejos ---
        for col in df.columns:
          # Si la columna es de tipo 'object' (potencialmente dict o list)
          if df[col].dtype == 'object':
            # Tomamos la primera celda no-nula para ver qué es
            first_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
              
            # Si es un dict o list, lo convertimos todo a string JSON
            if isinstance(first_val, (dict, list)):
              logging.info(f"Convirtiendo columna anidada '{col}' a JSON string para tabla '{table_name}'.")
              df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
        # --- FIN DEL CÓDIGO NUEVO ---

        # Añadir columna de inserción (Requisito)
        df['insertion_date'] = datetime.now().isoformat()
        
        # Cargar a Sqlite
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
          
      logging.info("Guardado en base de datos completado.")
          
  except Exception as e:
    logging.error(f"Error guardando en la base de datos: {e}")
    # (Opcional) imprimir más detalles del error
    import traceback
    logging.error(traceback.format_exc())

# ... (imports, incluyendo 'import time') ...

def upload_to_sftp(filepaths):
  """
  Sube los archivos generados a un servidor SFTP usando Llave SSH.
  Cumple con: Requisito SFTP
  """
  logging.info("Iniciando subida a SFTP...")
  
  sftp = None
  transport = None
  max_retries = 5
  
  # --- BUCLE DE REINTENTO AÑADIDO ---
  for attempt in range(max_retries):
    try:
      private_key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
      
      # Conectar
      transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
      transport.connect(username=SFTP_USER, pkey=private_key)
      sftp = paramiko.SFTPClient.from_transport(transport)
      
      logging.info(f"Conectado a SFTP en {SFTP_HOST}:{SFTP_PORT} (Intento {attempt+1})")
      break  # ¡Éxito! Salimos del bucle

    except Exception as e:
      logging.warning(f"Intento {attempt+1}/{max_retries} fallido. No se pudo conectar a SFTP: {e}. Reintentando en 5s...")
      if transport:
        transport.close()
      if attempt + 1 == max_retries:
        logging.error("Todos los intentos de conexión SFTP fallaron. Abortando subida.")
        return  # Nos rendimos
      time.sleep(5)
# --- FIN DEL BUCLE DE REINTENTO ---
          
  # Si estamos aquí, 'sftp' debería estar conectado
  if not sftp:
    logging.error("Fallo inesperado, la conexión SFTP no está activa.")
    return

  try:
    # Subir los 3 archivos
    files_to_upload = [
      filepaths['raw_file'],
      filepaths['processed_file'],
      filepaths['dlq_file']
    ]
    
    for local_path in files_to_upload:
      if not os.path.exists(local_path):
        logging.warning(f"Archivo no encontrado: {local_path}. Saltando subida a SFTP.")
        continue
          
      remote_filename = os.path.basename(local_path)
      remote_path = f"{SFTP_REMOTE_DIR}/{remote_filename}"
      
      logging.info(f"Subiendo {local_path} a {remote_path}...")
      sftp.put(local_path, remote_path)
        
    logging.info("Subida a SFTP completada.")
      
  except Exception as e:
    logging.error(f"Error durante la subida a SFTP (después de conectar): {e}")
  finally:
    # Cerrar conexión
    if sftp:
      sftp.close()
    if transport:
      transport.close()

def main():
  logging.info("Starting 'Saver' service...")

  time.sleep(5)

# --- Bucle de espera para Redis ---
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
      # r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
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