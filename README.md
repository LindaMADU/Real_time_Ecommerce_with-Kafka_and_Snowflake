# Real_time_Ecommerce_with-Kafka_and_Snowflake
🛒 Real-Time Ecommerce Streaming with Kafka and Snowflake
A real-time data engineering pipeline that streams ecommerce events using Apache Kafka, processes and stores them in Snowflake, and visualizes live insights on a Power BI dashboard.

📌 Project Overview
This project simulates a real-time ecommerce platform where user events (orders, clicks, payments) are streamed through Kafka, ingested and transformed in Snowflake, and displayed on a live Power BI dashboard for business intelligence.

❗ Business Problem
Modern ecommerce businesses generate thousands of customer events every second — product views, add-to-cart actions, checkouts, payments, and cancellations. Without a real-time data pipeline, these businesses face critical challenges:

Delayed decision making — traditional batch pipelines process data hours later, meaning teams react to problems long after they occur (e.g. a payment gateway failure discovered hours after it started)
No live visibility — business teams have no real-time view of sales performance, cart abandonment rates, or customer behavior as it happens
Lost revenue opportunities — without instant insights, businesses cannot trigger real-time promotions, detect fraud early, or respond to sudden spikes in demand
Data silos — event data is scattered across multiple systems with no unified view for analytics
Scalability bottlenecks — traditional databases cannot handle the high-throughput, continuous nature of ecommerce event streams

💡 The Solution
This project solves these problems by building an end-to-end real-time streaming pipeline that:

Captures every ecommerce event instantly using Apache Kafka as the streaming backbone
Processes and stores events in Snowflake using a Bronze → Silver → Gold layered architecture for clean, reliable data
Delivers live business insights to stakeholders through a Power BI dashboard connected directly to Snowflake
Scales horizontally to handle high event volumes without performance degradation
Decouples data producers from consumers so each component can evolve independently

Business ProblemSolution in This ProjectHours-old batch dataReal-time Kafka streamingNo live sales visibilityPower BI dashboard on live Snowflake viewsMessy raw event dataBronze → Silver → Gold transformation layersInability to scaleKafka partitions + Snowflake elastic computeSiloed event dataUnified Snowflake warehouse for all events

🏗️ Architecture
Ecommerce App (Producer)
        │
        ▼
  Apache Kafka
  (Topic: kafka_events)
        │
        ▼
  Kafka Consumer (Python)
        │
        ▼
  Snowflake (Bronze → Silver → Gold)
        │
        ▼
  Power BI Dashboard

🧰 Tech Stack
Apache Kafka
Snowflake
Python, Pandas
Git & GitHub

📁 Project Architectural Diagram

<img width="4272" height="1884" alt="image" src="https://github.com/user-attachments/assets/4f8f02a1-b273-4279-badc-92baf52567f9" />


⚙️ Setup & Installation
1. Clone the Repository
bashgit clone https://github.com/LindaMADU/Real_time_Ecommerce_with-Kafka_and_Snowflake.git
cd Real_time_Ecommerce_with-Kafka_and_Snowflake
2. Create and Activate Virtual Environment
bash# Create
python -m venv venv

# Activate (Windows CMD)
venv\Scripts\activate.bat

# Activate (Windows PowerShell)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate
3. Install Dependencies
bashpip install -r requirements.txt
4. Configure Environment Variables
Create a .env file in the root directory:
env# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=kafka_events

# Snowflake
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

🚀 Running the Pipeline
Step 1 — Start Kafka
bash# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
Step 2 — Run the Producer
bashpython producer/producer.py
Step 3 — Run the Consumer
bashpython consumer/consumer.py
Step 4 — Verify Data in Snowflake
sqlSELECT * FROM KAFKA_EVENTS_BRONZE LIMIT 10;
SELECT * FROM KAFKA_EVENTS_SILVER LIMIT 10;
SELECT * FROM KAFKA_EVENTS_GOLD   LIMIT 10;

❄️ Snowflake Data Layers
LayerTable/ViewDescriptionBronzeKAFKA_EVENTS_BRONZERaw events as received from KafkaSilverKAFKA_EVENTS_SILVERCleaned and transformed eventsGoldKAFKA_EVENTS_GOLDAggregated, business-ready views

📊 Power BI Dashboard
The dashboard connects to Snowflake via the native Snowflake connector and visualizes:

Total orders and revenue in real time
Orders by product category
Customer activity heatmap
Payment status breakdown
Top performing products

To open: Load powerbi/dashboard.pbix in Power BI Desktop and update the Snowflake connection credentials under Home → Transform Data → Data Source Settings.

📦 Requirements
confluent-kafka
snowflake-connector-python
pandas
python-dotenv
Install with:
bashpip install -r requirements.txt
