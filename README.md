# Technical Test - Backend (ETL Pipeline & API)

This project implements a complete ETL (Extract, Transform, Load) pipeline and a REST API as part of a technical test.

The pipeline extracts data from the [DummyJSON](https://dummyjson.com/users) API, transforms, validates, and enriches it. Finally, it loads the data into a Sqlite database and uploads it to an SFTP server. A FastAPI REST API exposes the saved data to be consumed by a frontend.

## Tech Stack
* **Language:** Python 3.10
* **API:** FastAPI
* **Data Processing:** Pandas
* **Database:** Sqlite
* **Message Queue:** Redis
* **SFTP:** Paramiko (client), `atmoz/sftp` (server)
* **Containers:** Docker & Docker Compose

## Architecture (Component Diagram)
This project uses a decoupled microservices architecture orchestrated by Docker Compose.

## ETL Pipeline Flow

```
(External API) -> [Extractor (Phase 1)] --(publishes)--> [Redis (Channel: channel:phase1_complete)]
                                                                |
                                                                v
                                                        [Transformer (Phase 2)] --(publishes)--> [Redis (Channel: channel:phase2_complete)]
                                                                                                |
                                                                                                v
                                                                                          [Saver (Phase 3)]
                                                                                             /      \
                                                                                            /        \
                                                                                           v          v
                                                                                     [Sqlite DB]   [SFTP Server]
```
### REST API Flow

```
(User) <--> [Frontend (Next.js)] <--> [REST API (FastAPI)] <--> [Sqlite DB]
```
## Programming Practices
* **Environment Variables:** All sensitive configuration (credentials, hosts) is managed externally via an `.env` file.
* **Dockerization:** The entire application is containerized and runs with a single command (`docker-compose up`), using lightweights (`alpine`) images.
* **Modular Code:** Each pipeline phase is an independent script and a separate Docker service, facilitating maintenance.
* **Robust Error Handling:**
    * **Extractor**: Implements retries with exponential backoff for network failures.
    * **Transformer**: Separates bad data into a DLQ without halting the process.
    * **Saver**: Uses separate database transactions to ensure data integrity.

---
## How to Run This Project
### Prerequisites
* [Git](https://git-scm.com/)
* [Docker](https://www.docker.com/)
* [Docker Compose](https://docs.docker.com/compose/)

### 1. Initial Setup

1.  **Clone the repository:**
    *(Remember to change the URL to your own repository)*
    
    ```bash
    git clone https://github.com/tu-usuario/proyecto-tecnico-backend.git cd proyecto-tecnico-backend
    ```
    
2.  **Create the environment file:**
    Copy the example file. No changes are needed for the default configuration.
    
    ```bash
    cp .env.example .env
    ```
    
3.  **Generate SSH Keys for SFTP (Mandatory!)::**
    This step creates the keys the `saver` will use to connect to the `sftp` server.
    
    ```bash
    1. Create the directory
    mkdir ssh_keys

    2. Generate the key pair (press Enter 3 times for no passphrase)
    ssh-keygen -t rsa -b 4096 -f ./ssh_keys/id_rsa -N ""

    3. Rename the public key for the SFTP server to recognize it
    mv ./ssh_keys/id_rsa.pub ./ssh_keys/sftp_user.pub
    ```
    
### 2. Run the Application

Use Docker Compose to build and run all services in the background (`-d`).

```bash
docker-compose up --build -d
```
That's it! The pipeline will now run automatically.

### 3. How to Test and Verify

1.  **Watch the pipeline logs in action:**
    You can "tail" the logs for each service in separate terminals.
    
    ```bash
    Watch the Extractor downloading
    docker-compose logs -f extractor

    Watch the Transformer validating (activates after extractor)
    docker-compose logs -f transformer

    Watch the Saver saving to DB and SFTP (activates after transformer)
    docker-compose logs -f saver
    ```
    
2.  **Verify the output (after a few minutes):**
    * **Database:** Check that the `database/data.db` file has been created.
    * **SFTP:** Check that the `sftp_data/` folder contains the 3 `.jsonl` files.
    * **API:** Open your browser or Postman and go to `http://localhost:8000/etl_runs`. You should receive a JSON response with the first run's data (not an empty `[]`).

---

# Proyecto Técnico - Backend (ETL Pipeline & API)

Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) completo y una API REST como parte de una prueba técnica.

El pipeline extrae datos de la API [DummyJSON](https://dummyjson.com/users), los transforma, valida y enriquece, y finalmente los carga en una base de datos **Sqlite** y los sube a un servidor **SFTP**. Una **API REST (FastAPI)** expone los datos guardados para ser consumidos por un frontend.

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
(API Externa) -> [Extractor (Fase 1)] --(publica)--> [Redis (Canal: channel:phase1_complete)]
                                                                |
                                                                v
                                                        [Transformador (Fase 2)] --(publica)--> [Redis (Canal: channel:phase2_complete)]
                                                                                                |
                                                                                                v
                                                                                        [Guardador (Fase 3)]
                                                                                             /      \
                                                                                            /        \
                                                                                           v          v
                                                                                     [Sqlite DB]   [Servidor SFTP]
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
    *(Cambiar la URL por la de tu repositorio)*
    
    ```bash
    git clone https://github.com/tu-usuario/proyecto-tecnico-backend.git cd proyecto-tecnico-backend
    ```
    
2.  **Crear el archivo de entorno:**
    Copiar el archivo de ejemplo. No se necesitan cambios para la configuración por defecto.
    
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

Usar Docker Compose para construir y levantar todos los servicios en segundo plano (`-d`).

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
