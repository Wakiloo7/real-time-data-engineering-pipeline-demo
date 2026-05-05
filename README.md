# End-to-End Real-Time Data Engineering Pipeline

## Project Overview

This project is an end-to-end real-time data engineering pipeline built with **Apache Airflow, Apache Kafka, Apache Spark, Apache Cassandra, PostgreSQL, Docker, Zookeeper, Schema Registry, and Confluent Control Center**.

The pipeline collects random user data from an external API, streams the data through Kafka, and prepares the infrastructure for real-time processing and storage.

I deployed and customized this project locally on Windows using Docker Desktop. I fixed several compatibility issues, updated outdated Docker images, resolved Airflow startup problems, configured the scheduler, verified the DAG execution, and documented the full working setup with screenshots.

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Docker | Containerized the full data engineering environment |
| Docker Compose | Orchestrated all services together |
| Apache Airflow | Workflow orchestration and DAG scheduling |
| PostgreSQL | Airflow metadata database |
| Apache Kafka | Real-time data streaming |
| Apache Zookeeper | Kafka coordination |
| Confluent Schema Registry | Kafka schema management |
| Confluent Control Center | Kafka monitoring UI |
| Apache Spark | Distributed data processing engine |
| Apache Cassandra | NoSQL storage layer |
| Python | Data extraction, formatting, and streaming logic |
| RandomUser API | External data source |

---

## System Architecture

The pipeline architecture is:

```text
RandomUser API
      ↓
Apache Airflow DAG
      ↓
Python data extraction and formatting
      ↓
Apache Kafka topic: users_created
      ↓
Apache Spark cluster
      ↓
Apache Cassandra
```

The supporting infrastructure includes:

```text
PostgreSQL → Airflow metadata database
Zookeeper → Kafka coordination
Schema Registry → Kafka schema support
Control Center → Kafka monitoring
Docker Compose → Container orchestration
```

---

## What This Project Demonstrates

This project demonstrates how to:

- Build a real-time data pipeline using modern data engineering tools.
- Use Airflow to orchestrate a data streaming task.
- Use Kafka as a message broker for streaming data.
- Run Kafka, Zookeeper, Spark, Cassandra, PostgreSQL, and Airflow using Docker.
- Monitor Kafka using Confluent Control Center.
- Monitor Spark using the Spark Master UI.
- Debug real-world Docker and Airflow compatibility issues.
- Deploy a multi-container data engineering stack locally on Windows.

---

## My Contributions

The original repository had several issues when running locally on my Windows machine. I fixed and customized the project to make it work.

### 1. Fixed Docker permission/location issue

At first, I tried cloning the repository inside:

```text
C:\WINDOWS\system32
```

This caused a permission error:

```text
fatal: could not create work tree dir 'e2e-data-engineering': Permission denied
```

I fixed this by cloning the project into my user folder:

```cmd
cd C:\Users\md.w.ahmad\Documents\Job apply\New folder
mkdir Projects
cd Projects
git clone https://github.com/airscholar/e2e-data-engineering.git
cd e2e-data-engineering
```

---

### 2. Fixed broken Spark Docker image

The original project used:

```yaml
image: bitnami/spark:latest
```

This failed with:

```text
failed to resolve reference "docker.io/bitnami/spark:latest": not found
```

I replaced it with:

```yaml
image: bitnamilegacy/spark:latest
```

Updated Spark services:

```yaml
spark-master:
  image: bitnamilegacy/spark:latest
  command: bin/spark-class org.apache.spark.deploy.master.Master
  ports:
    - "9090:8080"
    - "7077:7077"

spark-worker:
  image: bitnamilegacy/spark:latest
  command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

---

### 3. Fixed Airflow entrypoint issue on Windows

The original Airflow webserver used:

```yaml
entrypoint: ['/opt/airflow/script/entrypoint.sh']
```

On Windows, this caused:

```text
exec /opt/airflow/script/entrypoint.sh: no such file or directory
```

I fixed this by removing the problematic shell script entrypoint and running the Airflow setup directly inside the Docker Compose command.

Updated Airflow webserver command:

```yaml
command: >
  bash -c "
  pip install -r /opt/airflow/requirements.txt &&
  airflow db upgrade &&
  airflow users create
  --username admin
  --firstname admin
  --lastname admin
  --role Admin
  --email admin@example.com
  --password admin || true &&
  airflow webserver
  "
```

---

### 4. Fixed Airflow scheduler startup

The scheduler was initially created but not running:

```text
scheduler Created
```

I started it manually using:

```cmd
docker compose up -d scheduler
```

Then verified it was running:

```cmd
docker compose ps
```

The scheduler logs confirmed:

```text
Starting the scheduler
Launched DagFileProcessorManager
```

---

### 5. Changed DAG owner

The original DAG owner was:

```python
'owner': 'airscholar'
```

I changed it to my name:

```python
'owner': 'md.w.ahmad'
```

Current Airflow DAG list confirms:

```text
dag_id          | filepath        | owner      | paused
================+=================+============+=======
user_automation | kafka_stream.py | md.w.ahmad | False
```

---

## Airflow DAG Code

The main DAG file is:

```text
dags/kafka_stream.py
```

It defines the DAG:

```python
with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
```

The main task is:

```text
stream_data_from_api
```

This task:

1. Calls the RandomUser API.
2. Extracts one random user record.
3. Formats the user data.
4. Sends the formatted data to Kafka topic:

```text
users_created
```

---

## Data Flow Explanation

### Step 1: Airflow triggers the DAG

Airflow runs the DAG named:

```text
user_automation
```

The DAG has one Python task:

```text
stream_data_from_api
```

---

### Step 2: Python fetches data from API

The function `get_data()` calls:

```text
https://randomuser.me/api/
```

It receives random user data in JSON format.

---

### Step 3: Data is formatted

The function `format_data()` extracts and structures fields such as:

```text
id
first_name
last_name
gender
address
post_code
email
username
dob
registered_date
phone
picture
```

---

### Step 4: Data is sent to Kafka

The function `stream_data()` creates a Kafka producer:

```python
producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
```

It sends data to the Kafka topic:

```text
users_created
```

---

### Step 5: Kafka stores streaming messages

Kafka receives the messages and makes them available for downstream processing.

---

### Step 6: Spark cluster is available for processing

Spark Master and Spark Worker are running inside Docker.

Spark UI confirms:

```text
Spark Master: ALIVE
Workers: 1 Alive
Cores: 2 Total
Memory: 1024 MiB Total
```

---

### Step 7: Cassandra is available for storage

Cassandra is running as the NoSQL database layer and exposed on:

```text
localhost:9042
```

---

## How to Run This Project Locally

### Prerequisites

Install:

- Git
- Docker Desktop
- Windows WSL2 backend for Docker
- Web browser
- Command Prompt or PowerShell

Recommended Docker resources:

```text
RAM: 8 GB minimum, 12–16 GB preferred
CPU: 2+ cores
```

---

## Setup Instructions

### 1. Clone the repository

```cmd
git clone https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git
cd YOUR_REPOSITORY_NAME
```

---

### 2. Start all services

```cmd
docker compose up --build
```

Or run in background:

```cmd
docker compose up -d --build
```

---

### 3. Check running containers

```cmd
docker compose ps
```

Expected services:

```text
broker
cassandra
control-center
postgres
scheduler
spark-master
spark-worker
webserver
schema-registry
zookeeper
```

---

### 4. Open Airflow

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

---

### 5. Check DAGs

Command line:

```cmd
docker compose exec webserver airflow dags list
```

Expected output:

```text
dag_id          | filepath        | owner      | paused
================+=================+============+=======
user_automation | kafka_stream.py | md.w.ahmad | False
```

---

### 6. Unpause the DAG

```cmd
docker compose exec webserver airflow dags unpause user_automation
```

---

### 7. Trigger the DAG

```cmd
docker compose exec webserver airflow dags trigger user_automation
```

---

### 8. Check DAG runs

```cmd
docker compose exec webserver airflow dags list-runs -d user_automation
```

---

### 9. Check task state

```cmd
docker compose exec webserver airflow tasks states-for-dag-run user_automation scheduled__2026-05-03T00:00:00+00:00
```

Example output:

```text
dag_id          | task_id              | state
================+======================+========
user_automation | stream_data_from_api | running
```

---

## Service URLs

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Confluent Control Center | http://localhost:9021 |
| Spark UI | http://localhost:9090 |
| Kafka Broker | localhost:9092 |
| Cassandra | localhost:9042 |
| Zookeeper | localhost:2181 |

---

## Output Screenshots

### 1. Docker Compose Services Running

This screenshot shows all services running through Docker Compose:

```text
Kafka Broker
Zookeeper
Schema Registry
Confluent Control Center
Airflow Webserver
Airflow Scheduler
PostgreSQL
Spark Master
Spark Worker
Cassandra
```

![Docker Compose Services](images/docker-compose-ps.png)

---

### 2. Airflow DAG Owner Updated

This screenshot shows the DAG owner changed to:

```text
MD.W.AHMAD
```

![Airflow DAG Owner](images/airflow-dag-owner.png)

---

### 3. Airflow DAG Grid

This screenshot shows the Airflow DAG execution status for:

```text
user_automation
```

Task:

```text
stream_data_from_api
```

![Airflow DAG Grid](images/airflow-grid.png)

---

### 4. Confluent Control Center

This screenshot shows the Kafka cluster running in Confluent Control Center.

It confirms:

```text
1 Healthy cluster
0 Unhealthy clusters
```

![Confluent Control Center](images/confluent-control-center.png)

---

### 5. Spark UI

This screenshot shows the Spark Master UI running successfully.

It confirms:

```text
Spark Master: ALIVE
Workers: 1 Alive
Cores: 2 Total
Memory: 1024 MiB Total
```

![Spark UI](images/spark-ui.png)

---

### 6. Schema Registry

Schema Registry is available at:

```text
http://localhost:8081
```

Example response:

```json
{}
```

![Schema Registry](images/schema-registry.png)

---

## Important Commands

### Start containers

```cmd
docker compose up -d
```

### Stop containers

```cmd
docker compose down
```

### Stop and remove volumes

```cmd
docker compose down -v
```

### Rebuild containers

```cmd
docker compose up --build
```

### Check container status

```cmd
docker compose ps
```

### View Airflow webserver logs

```cmd
docker compose logs webserver
```

### View Airflow scheduler logs

```cmd
docker compose logs scheduler
```

### View Kafka broker logs

```cmd
docker compose logs broker
```

### View Spark master logs

```cmd
docker compose logs spark-master
```

---

## Troubleshooting

### Problem 1: Permission denied while cloning

Error:

```text
fatal: could not create work tree dir 'e2e-data-engineering': Permission denied
```

Cause:

The repository was being cloned inside:

```text
C:\WINDOWS\system32
```

Fix:

Clone into a user folder instead:

```cmd
cd C:\Users\md.w.ahmad\Documents
mkdir Projects
cd Projects
git clone https://github.com/airscholar/e2e-data-engineering.git
```

---

### Problem 2: Spark image not found

Error:

```text
docker.io/bitnami/spark:latest: not found
```

Fix:

Replace:

```yaml
image: bitnami/spark:latest
```

With:

```yaml
image: bitnamilegacy/spark:latest
```

---

### Problem 3: Airflow entrypoint not found

Error:

```text
exec /opt/airflow/script/entrypoint.sh: no such file or directory
```

Cause:

The original project used a mounted Linux shell script as the Airflow entrypoint. On Windows, this caused execution issues.

Fix:

Removed the entrypoint and ran Airflow commands directly in Docker Compose.

---

### Problem 4: Airflow scheduler not running

Error in Airflow UI:

```text
The scheduler does not appear to be running.
The DAGs list may not update, and new tasks will not be scheduled.
```

Fix:

Start scheduler manually:

```cmd
docker compose up -d scheduler
```

Check logs:

```cmd
docker compose logs scheduler
```

Successful scheduler log:

```text
Starting the scheduler
Launched DagFileProcessorManager
```

---

### Problem 5: DAG not visible in Airflow UI

Check whether DAG exists inside the container:

```cmd
docker compose exec webserver ls -la /opt/airflow/dags
```

Check Airflow DAG list:

```cmd
docker compose exec webserver airflow dags list
```

Check import errors:

```cmd
docker compose exec webserver airflow dags list-import-errors
```

Expected result:

```text
No data found
```

---

### Problem 6: Protobuf dependency warning

Error:

```text
TypeError: Descriptors cannot not be created directly.
```

Possible fix:

Add this to `requirements.txt`:

```text
protobuf==3.20.3
```

Then restart:

```cmd
docker compose down
docker compose up -d --build
```

---

## Current Working Status

The project currently runs successfully with the following status:

```text
Airflow webserver: healthy
Airflow scheduler: running
Kafka broker: healthy
Zookeeper: healthy
Schema Registry: healthy
Control Center: healthy
Spark Master: running
Spark Worker: running
Cassandra: running
PostgreSQL: running
```

The Airflow DAG is visible:

```text
user_automation
```

The DAG owner is:

```text
md.w.ahmad
```

The DAG task is:

```text
stream_data_from_api
```

The DAG status is:

```text
unpaused / running
```


## Key Learning Outcomes

From this project, I learned:

- How to run a full data engineering platform with Docker Compose.
- How Airflow schedules and manages DAGs.
- How Kafka producers send streaming messages.
- How Zookeeper supports Kafka.
- How Schema Registry and Control Center are used in Kafka ecosystems.
- How Spark Master and Workers are structured.
- How Cassandra fits into a real-time data pipeline.
- How to debug Docker, Airflow, and dependency issues.
- How to verify services using logs, UI dashboards, and CLI commands.
- How to document a data engineering project for GitHub and interviews.

---

## Project Folder Structure

```text
e2e-data-engineering/
│
├── dags/
│   └── kafka_stream.py
│
├── script/
│   └── entrypoint.sh
│
├── images/
│   ├── docker-compose-ps.png
│   ├── airflow-dag-owner.png
│   ├── airflow-grid.png
│   ├── confluent-control-center.png
│   ├── spark-ui.png
│   └── schema-registry.png
│
├── docker-compose.yml
├── requirements.txt
├── spark_stream.py
├── README.md
└── .gitignore
```

---

## Future Improvements

Possible improvements for this project:

- Add a Spark Structured Streaming job to consume from Kafka.
- Write processed Spark output into Cassandra.
- Add Kafka topic validation screenshots.
- Add unit tests for the Python formatting function.
- Add logging for every successful Kafka message.
- Create a custom Airflow Docker image instead of installing requirements at runtime.
- Add a `.env` file for configuration.
- Add GitHub Actions for basic validation.
- Add architecture diagram created by me.
- Add sample output records.

---