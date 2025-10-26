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

app = FastAPI(title="Technical Test API", version="1.0")

# --- Configuración de CORS ---
# Permite que el frontend pueda llamar a la api
app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

# --- Dependencia de Base de Datos ---
def get_db_connection():
  if not os.path.exists(DB_PATH):
    raise HTTPException(status_code=503, detail="The database hasn't been created yet. Run the pipeline first.")
  
  conn = None
  try:
    conn = DB_ENGINE.connect()
    yield conn
  finally:
    if conn:
      conn.close()

# --- Modelos de Datos ---
class EtlRun(BaseModel):
  id: int
  run_timestamp: str
  processed_file: str
  valid_count: int
  invalid_count: int

# --- Endpoints ---

@app.get("/")
def read_root():
  return {"status": "Pipeline API working"}

@app.get("/etl_runs", response_model=List[EtlRun])
def get_etl_runs(db: Connection = Depends(get_db_connection)):
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
    logging.error(f"Error get runs: {e}")
    # Si la tabla 'etl_runs' aún no existe
    raise HTTPException(status_code=500, detail=f"Error consulting the database: {e}")

@app.get("/data/{table_name}", response_model=List[Dict[str, Any]])
def get_data_by_table(table_name: str, db: Connection = Depends(get_db_connection)):
  allowed_tables = ["raw_users", "processed_users", "invalid_users"]
  if table_name not in allowed_tables:
    raise HTTPException(status_code=400, detail="TableName not Allowed. Valid names: raw_users, processed_users, invalid_users.")
      
  try:
    query = text(f"SELECT * FROM {table_name} LIMIT 100") 
    result = db.execute(query)
    
    # Convertir el resultado de SQLAlchemy a un diccionario
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in result.fetchall()]
    return data
      
  except Exception as e:
    logging.error(f"Error al obtener datos de {table_name}: {e}")
    raise HTTPException(status_code=500, detail=f"Error al consultar la tabla {table_name}: {e}")

# --- Endpoint para el la SQL Box ---
class SqlQuery(BaseModel):
  query: str

@app.post("/query_sql")
def execute_sql_query(query_data: SqlQuery, db: Connection = Depends(get_db_connection)):
  query = query_data.query.strip().upper()
  
  # Una validación de seguridad. (muy básica)
  if not query.startswith("SELECT"):
    raise HTTPException(status_code=400, detail="Only SELECT querys are allowed.")
      
  try:
    result = db.execute(text(query_data.query))
    
    # Para consultas SELECT, devuelve los datos
    if query.startswith("SELECT"):
      column_names = result.keys()
      data = [dict(zip(column_names, row)) for row in result.fetchall()]
      return {"columns": list(column_names), "rows": data}
    else:
      # Para INSERT, UPDATE, DELETE... en caso de permitirse
      return {"message": "Query executed.", "row_count": result.rowcount}
          
  except Exception as e:
    logging.error(f"Error executing SQL: {e}")
    raise HTTPException(status_code=400, detail=f"SQL error: {str(e)}")