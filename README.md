# 🚢 Los Angeles Port Performance Monitor

End-to-end data pipeline transforming weekly Port Optimizer updates into operational congestion intelligence for the Port of Los Angeles.

# 📌 Project Overview

This project builds a structured, production-style monitoring system that:

📥 Extracts weekly data from Port Optimizer (Los Angeles)
  
🗄 Loads structured datasets into Azure SQL

📊 Connects to Power BI for modeling & transformation

🔄 Delivers a refresh-ready dashboard for weekly operational review

This is not a static report.
It is a repeatable supply chain monitoring pipeline.

## 🎯 Business Objective

Designed for a senior supply chain analyst who needs visibility every Monday morning.

Key questions answered:

Is port congestion increasing?

Are transit times trending above baseline?

Is rail moving slower than the last few weeks?

Should additional trucking capacity be secured?

Are customs delays expected?

The goal is proactive decision support — not reactive reporting.

# 🖼 Dashboard Preview

<img width="1312" height="737" alt="los_angeles_port_perf" src="https://github.com/user-attachments/assets/0d9e9d19-386d-4154-8284-e19eabe01112" />


## 🏗 Architecture

Port Optimizer (Weekly Source)
→ Python Extraction
→ Azure SQL Storage
→ Power BI Model + Power Query
→ Operational Dashboard

The structure allows weekly refresh and scalability to additional ports.

## 📊 Data Scope – Port of Los Angeles

The dataset includes operational indicators such as:

Volume pressure metrics

Slow container / dwell trends

Terminal congestion signals

Berth activity

Weekly terminal status

Each extraction is timestamped and structured before loading into SQL.

# 🧠 What This Demonstrates

Production-style data engineering thinking

Cloud-based storage integration

Business-aligned metric design

Operational dashboard architecture

Supply chain decision intelligence

# 🛠 Tech Stack

🐍 Python (data extraction)

☁ Azure SQL (data storage & querying)

📈 Power BI (data modeling & visualization)

🔄 Power Query (data transformation)

## 📂 Project Structure
la-port-performance-monitor/
│
├── la_port_performance_pipeline.py
├── README.md
├── .env.example
└── outputs/

## 🚀 How to Run

1️⃣ Configure environment variables (see .env.example)
2️⃣ Run the Python extraction pipeline
3️⃣ Connect Power BI to Azure SQL
4️⃣ Refresh dataset

## 🔮 Future Improvements

Automated scheduled extraction

Congestion spike alerting

Multi-port comparison layer

Predictive transit modeling
