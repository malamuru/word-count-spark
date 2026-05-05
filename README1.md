<h1 align="center">⚡ Word Count using Apache Spark on AWS S3</h1>

<h3 align="center">
PySpark | AWS EC2 | S3 | Distributed Data Processing
</h3>

<p align="center"><em>"Processing text data from cloud storage using distributed computing with Apache Spark."</em></p>

---

## ✨ Overview

This project implements a **distributed word count application** using **Apache Spark and PySpark** on AWS.

The system reads text data from **Amazon S3**, processes it using Spark transformations, and stores the output back in S3.

It demonstrates foundational concepts in **big data processing and cloud-based analytics**.

---

## 🚀 Key Features

- ☁️ Reads input data directly from AWS S3  
- ⚡ Processes large datasets using Apache Spark  
- 🔤 Performs word tokenization and counting  
- 📊 Generates word frequency output  
- 📁 Stores processed results back in S3  

---

## 🧠 Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)

---

## 📂 Project Structure
- word_count.py # PySpark word count script
- input_data/ # Input text file (S3)
- output/ # Output word count results (S3)


---

## 🔍 Processing Workflow

1. Upload text file to S3  
2. Read data using PySpark  
3. Split text into words  
4. Count occurrences using `reduceByKey()`  
5. Store results back in S3  

---

### 📌 Project Highlights
- Built a distributed data processing application
- Integrated Apache Spark with AWS S3
- Used Spark transformations (flatMap, map, reduceByKey)
- Demonstrated cloud-based big data workflow

---
