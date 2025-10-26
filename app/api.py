import os
import logging
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from pydantic import BaseModel
from typing import List, Dict, Any

# --- Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = '/app/database/data.db'
# Usamos check_same_thread=False solo porque es Sqlite
DB_ENGINE = create_engine(f'sqlite:///{DB_PATH}', connect_args={"check_same_thread": False})

app = FastAPI(title="API de la Prueba Técnica", version="1.0")

# --- Configuración de CORS ---
# Permite que tu frontend (que estará en otro dominio) hable con este API
app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],  # En producción, cámbialo a tu dominio de frontend
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

# --- Dependencia de Base de Datos ---
def get_db_connection():
  """Maneja la conexión a la base de datos por cada request."""
  if not os.path.exists(DB_PATH):
    raise HTTPException(status_code=503, detail="La base de datos aún no ha sido creada. Ejecuta el pipeline primero.")
  
  conn = None
  try:
    conn = DB_ENGINE.connect()
    yield conn
  finally:
    if conn:
      conn.close()

# --- Modelos de Datos (para el response) ---
class EtlRun(BaseModel):
  id: int
  run_timestamp: str
  processed_file: str
  valid_count: int
  invalid_count: int

# --- Endpoints ---

@app.get("/")
def read_root():
  return {"status": "API del pipeline funcionando"}

@app.get("/etl_runs", response_model=List[EtlRun])
def get_etl_runs(db: Connection = Depends(get_db_connection)):
  """
  Obtiene la lista de corridas del ETL (la "header table").
  Esto se usará para el menú desplegable del frontend. 
  """
  try:
    query = text("SELECT id, run_timestamp, processed_file, valid_count, invalid_count FROM etl_runs ORDER BY run_timestamp DESC")
    result = db.execute(query).fetchall()
    
    # Mapea los resultados a un formato de lista de diccionarios
    runs = [
      {
        "id": row[0],
        "run_timestamp": row[1],
        "processed_file": row[2],
        "valid_count": row[3],
        "invalid_count": row[4]
      } for row in result
    ]
    return runs
  except Exception as e:
    logging.error(f"Error al obtener corridas: {e}")
    # Esto sucede si la tabla 'etl_runs' aún no existe
    raise HTTPException(status_code=500, detail=f"Error al consultar la base de datos: {e}")

@app.get("/data/{table_name}", response_model=List[Dict[str, Any]])
def get_data_by_table(table_name: str, db: Connection = Depends(get_db_connection)):
  """
  Obtiene los datos de una tabla específica (raw_users, processed_users, invalid_users).
  
  NOTA: Esto es solo un ejemplo. La prueba muestra una caja de SQL,
  lo cual es una MALA PRÁCTICA de seguridad (Inyección SQL).
  Un endpoint como este es mucho más seguro.
  """
  allowed_tables = ["raw_users", "processed_users", "invalid_users"]
  if table_name not in allowed_tables:
    raise HTTPException(status_code=400, detail="Nombre de tabla no permitido. Solo se permite: raw_users, processed_users, invalid_users.")
      
  try:
    # Usamos f-string de forma SEGURA porque hemos validado 'table_name'
    query = text(f"SELECT * FROM {table_name} LIMIT 100") 
    result = db.execute(query)
    
    # Convertir el resultado de SQLAlchemy a un diccionario
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in result.fetchall()]
    return data
      
  except Exception as e:
    logging.error(f"Error al obtener datos de {table_name}: {e}")
    raise HTTPException(status_code=500, detail=f"Error al consultar la tabla {table_name}: {e}")

# --- Endpoint para el Requisito de la Caja SQL (¡INSEGURO!) ---
class SqlQuery(BaseModel):
  query: str

@app.post("/query_sql")
def execute_sql_query(query_data: SqlQuery, db: Connection = Depends(get_db_connection)):
  """
  Ejecuta una consulta SQL arbitraria enviada por el usuario.
  
  *** ADVERTENCIA DE SEGURIDAD ***
  Este endpoint es EXTREMADAMENTE INSEGURO y vulnerable a Inyección SQL.
  Solo se incluye para cumplir con el diagrama de la prueba.
  En un proyecto real, NUNCA se debe hacer esto.
  Se debe validar y parametrizar toda entrada del usuario.
  """
  query = query_data.query.strip().upper()
  
  # Una validación de seguridad MUY BÁSICA (insuficiente para producción)
  if not query.startswith("SELECT"):
    raise HTTPException(status_code=400, detail="Solo se permiten consultas SELECT.")
      
  try:
    result = db.execute(text(query_data.query))
    
    # Para consultas SELECT, devuelve datos
    if query.startswith("SELECT"):
      column_names = result.keys()
      data = [dict(zip(column_names, row)) for row in result.fetchall()]
      return {"columns": list(column_names), "rows": data}
    else:
      # Para INSERT, UPDATE, DELETE (si se permitieran)
      return {"message": "Consulta ejecutada.", "row_count": result.rowcount}
          
  except Exception as e:
    logging.error(f"Error ejecutando SQL: {e}")
    raise HTTPException(status_code=400, detail=f"Error de SQL: {str(e)}")