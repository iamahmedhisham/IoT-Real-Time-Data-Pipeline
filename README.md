# 🌾 Real-time Agricultural Data Pipeline for Smart Irrigation in Egypt

**A cloud-native IoT data pipeline designed to optimize water usage and improve rice cultivation efficiency in Egypt’s Nile Delta using AWS services, IoT sensors, and real-time analytics.**

---

## 📌 Overview
This project implements a **real-time agricultural data pipeline** that collects, processes, stores, and visualizes environmental and soil data to support **smart irrigation** decisions for rice farming in Egypt.  
It addresses **water scarcity, inefficient irrigation, and soil degradation** by leveraging **IoT sensors**, **AWS cloud services**, and **data analytics**.

The system enables farmers to make **data-driven decisions** by integrating:
- Continuous **soil & weather monitoring**.  
- **Real-time data validation and processing**.  
- **Intelligent irrigation recommendations**.  
- **Interactive dashboards** for decision support.

---

## 🏗️ Architecture

![Pipeline Architecture](Images/pipeline.drawio-6.svg)

---

## ⚙️ Tech Stack

- **IoT Devices** → Soil moisture, temperature, pH, NPK, wind speed/direction.  
- **AWS IoT Core** → Secure device connection and data ingestion.  
- **Amazon Kinesis Data Streams** → Real-time data streaming.  
- **AWS Lambda** → Data parsing, validation, enrichment, and error handling.  
- **Amazon S3** → Data lake storage (raw, processed, and invalid records).  
- **AWS Glue** → ETL processing and schema cataloging.  
- **Amazon Redshift** → Data warehousing with a **star schema** (Fact + Dimensions).  
- **Power BI** → Interactive dashboards and visualization.  
- **AWS CloudWatch & SNS** → Monitoring, logging, and alerts.  
- **Amazon OpenSearch** → Advanced log analysis.

---

## 📂 Data Flow Summary

1. **Data Ingestion**
   - IoT sensors publish data to **AWS IoT Core**.  
   - Data is routed to **Kinesis Data Streams**.

2. **Real-time Processing**
   - **AWS Lambda** validates, cleans, and enriches data.  
   - Valid records go to **S3 Data Lake**; invalid ones trigger **SNS alerts**.

3. **ETL & Storage**
   - **AWS Glue Jobs** transform and load data into **Redshift**.  
   - Incremental loads based on `event_id`.

4. **Visualization**
   - **Power BI** dashboards for water usage efficiency, nutrient levels, and yield predictions.

5. **Monitoring**
   - **CloudWatch**, **SNS**, and **OpenSearch** for performance tracking and troubleshooting.

---

## 📊 Key Features

- **Low-latency data processing** for real-time irrigation decisions.  
- **Automated data validation** to ensure accuracy.  
- **Star schema design** for analytical efficiency.  
- **Incremental ETL loading** to optimize performance.  
- **Interactive dashboards** for actionable insights.  
- **Scalable, cloud-native architecture** adaptable to other crops and regions.

---

## 🚀 How to Run

> **Note:** This project is built on AWS and requires relevant service permissions.

1. **Deploy IoT Sensors** in the field with MQTT publishing.  
2. **Configure AWS IoT Core Rules** to route data to Kinesis.  
3. **Set up AWS Lambda** for validation and transformation.  
4. **Create S3 Buckets** for raw and invalid data.  
5. **Run AWS Glue Crawler** to catalog S3 data.  
6. **Execute AWS Glue ETL Jobs** to load data into Redshift.  
7. **Connect Power BI** to Redshift for reporting.  
8. **Configure CloudWatch & SNS** for monitoring and alerts.

---

## 🌱 Benefits

- 💧 **Water Conservation** → Optimized irrigation reduces waste.  
- 🌾 **Increased Crop Yields** → Timely, data-backed decisions.  
- 💰 **Reduced Costs** → Efficient use of resources and labor.  
- 🔍 **Proactive Farm Management** → Early detection of issues.  
- 🌍 **Sustainability** → Environmentally responsible practices.

**Mentor:** Eng. Ibrahim El-Shal

---

## 📅 Date
**09-08-2025**
