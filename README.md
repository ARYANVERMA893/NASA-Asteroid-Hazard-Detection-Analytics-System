# NASA-Asteroid-Hazard-Detection-Analytics-System

# 🚀 Real-Time NASA NEO Hazard Detection Pipeline

This project builds a real-time asteroid hazard detection system using NASA’s Near-Earth Object (NEO) API. Asteroid close-approach data is streamed via Apache Kafka and processed using PySpark Structured Streaming. During feature engineering, nested JSON fields are flattened and physics-inspired features such as impact energy proxy (diameter × velocity²) and a distance-normalized risk score (impact energy / miss distance) are created to better capture potential threat severity. A trained Scikit-learn logistic regression model performs live hazard probability prediction, and enriched results are stored in AWS S3 for downstream analytics and Power BI visualization. The system demonstrates an end-to-end streaming ML architecture integrating data engineering, real-time inference, cloud storage, and BI reporting.

Architecture:
NASA API → Kafka → PySpark Streaming → ML Inference (scikit-learn) → AWS S3 -> Athena -> Power BI
