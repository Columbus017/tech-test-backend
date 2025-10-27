# Proyecto Técnico - Backend (ETL Pipeline & API)

Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) completo y una API REST como parte de una prueba técnica.

El pipeline extrae datos de la API [DummyJSON](https://dummyjson.com/users), los transforma, valida y enriquece, y finalmente los carga en una base de datos **Sqlite** y los sube a un servidor **SFTP**. Una **API REST (FastAPI)** expone los datos guardados para ser consumidos por un frontend.

## Características

* **Fase 1 (Extractor):** Script resiliente que extrae usuarios con paginación, reintentos (exponential backoff) y guardado de estado (resumible).
* **Fase 2 (Transformador):** Servicio independiente que valida esquemas de datos, enriquece los registros y envía los datos inválidos a una "Dead-Letter Queue" (DLQ).
* **Fase 3 (Guardador):** Servicio que guarda los 3 archivos (`raw`, `processed`, `dlq`) en la base de datos Sqlite y los sube a un SFTP.
* **API REST:** Un servidor FastAPI que expone endpoints (`/etl_runs`, `/query_sql`) para consultar la base de datos.

### Requisitos Mandatorios Cumplidos
* **Desacoplamiento con Cola de Mensajes:** Los servicios (Extractor, Transformador, Guardador) están desacoplados usando **Redis Pub/Sub**.
* **SFTP con Autenticación Segura:** La subida de archivos se realiza a un servidor SFTP usando **autenticación por llave SSH** (sin contraseñas).

## Stack Tecnológico

* **Lenguaje:** Python 3.10
* **API:** FastAPI
* **Procesamiento de Datos:** Pandas
* **Base de Datos:** Sqlite
* **Cola de Mensajes:** Redis
* **SFTP:** Paramiko (para cliente), `atmoz/sftp` (para servidor)
* **Contenedores:** Docker & Docker Compose

## Arquitectura (Diagrama de Componentes)

Este proyecto utiliza una arquitectura de microservicios desacoplados orquestada por Docker Compose.

### Flujo del Pipeline ETL
```
(API Externa) -> [Extractor (Fase 1)] --(publica)--> [Redis (Canal: channel:phase1_complete)] | v [Transformador (Fase 2)] --(publica)--> [Redis (Canal: channel:phase2_complete)] | v [Guardador (Fase 3)] /

/

v v [Sqlite DB] [Servidor SFTP]
```

### Flujo del API REST
```
(Usuario) <--> [Frontend (Next.js)] <--> [API REST (FastAPI)] <--> [Sqlite DB]
```

## Prácticas de Programación

* **Variables de Entorno:** Toda la configuración sensible (credenciales, hosts) se maneja externamente a través de un archivo `.env`.
* **Dockerización:** Toda la aplicación está contenida y se ejecuta con un solo comando (`docker-compose up`), usando imágenes ligeras (`alpine`).
* **Código Modular:** Cada fase del pipeline es un script independiente y un servicio de Docker separado, facilitando el mantenimiento.
* **Manejo de Errores Robusto:**
    * **Extractor:** Implementa reintentos con *exponential backoff* para fallos de red.
    * **Transformador:** Separa los datos erróneos en una DLQ sin detener el proceso.
    * **Guardador:** Usa transacciones de base de datos separadas para asegurar la integridad de los datos.

---

## Cómo Ejecutar el Proyecto

### Requisitos Previos

* [Git](https://git-scm.com/)
* [Docker](https://www.docker.com/)
* [Docker Compose](https://docs.docker.com/compose/)

### 1. Configuración Inicial

1.  **Clonar el repositorio:**
    *(Recuerda cambiar la URL por la de tu repositorio)*
    ```bash
    git clone https://github.com/tu-usuario/proyecto-tecnico-backend.git cd proyecto-tecnico-backend
    ```
2.  **Crear el archivo de entorno:**
    Copia el archivo de ejemplo. No se necesitan cambios para la configuración por defecto.
    ```bash
    cp .env.example .env
    ```
3.  **Generar las llaves SSH para el SFTP (¡Obligatorio!):**
    Este paso crea las llaves que el `saver` usará para conectarse al `sftp`.
    ```bash
    1. Crear la carpeta
    mkdir ssh_keys

    2. Generar el par de llaves (presiona Enter 3 veces para no usar contraseña)
    ssh-keygen -t rsa -b 4096 -f ./ssh_keys/id_rsa -N ""

    3. Renombrar la llave pública para que el servidor SFTP la reconozca
    mv ./ssh_keys/id_rsa.pub ./ssh_keys/sftp_user.pub
    ```

### 2. Ejecutar la Aplicación

Usa Docker Compose para construir y levantar todos los servicios en segundo plano (`-d`).
```bash
docker-compose up --build -d
```

### 3. Cómo Probar y Verificar

1.  **Ver los logs del pipeline en acción:**
    Puedes "escuchar" los logs de cada servicio en terminales separadas.
    ```bash
    Ver al Extractor descargar
    docker-compose logs -f extractor

    Ver al Transformador validar (se activa después del extractor)
    docker-compose logs -f transformer

    Ver al Guardador salvar en la DB y SFTP (se activa después del transformador)
    docker-compose logs -f saver
    ```
2.  **Verificar la salida (después de unos minutos):**
    * **Base de Datos:** Revisa que el archivo `database/data.db` haya sido creado.
    * **SFTP:** Revisa que la carpeta `sftp_data/` contenga los 3 archivos `.jsonl`.
    * **API:** Abre tu navegador o Postman y ve a `http://localhost:8000/etl_runs`. Deberías recibir una respuesta JSON con los datos de la primera corrida (no un `[]` vacío).
