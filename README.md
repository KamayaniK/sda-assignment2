\# Food Delivery Streaming Analytics



\## Assignment 3



This project implements an end-to-end streaming analytics pipeline for food-delivery data.



\### Pipeline

Zomato Dataset → Python Producer → Apache Kafka → Python Consumer → MySQL → Grafana



\### Kafka Topic

food-delivery-orders



\### Data

100 food-delivery orders generating 300 streaming lifecycle events.



\### Consumer

consumer.py consumes events from Kafka and stores the consumed records in consumed\_orders.csv.



\### Dashboard

Grafana is used to analyse delivery performance through KPI metrics and charts.



\### Dashboard Metrics

\- Total Orders

\- Average Delivery Time

\- Average Delivery Rating

\- Average Delivery Time by Traffic Density

\- Average Delivery Time by City

\- Average Delivery Time by Weather

\- Average Delivery Time by Vehicle Type

