@echo off
setlocal

REM === STEP 1: Navigate to your project folder ===
cd /d "C:\Users\bsilv\Desktop\Cultural-heritage-bigdata-project\docker-compose"

echo [1] Stopping existing Docker containers...
docker-compose down

echo [2] Building and starting containers...
docker-compose up --build -d

echo [3] Waiting for Kafka and Zookeeper to stabilize...
timeout /t 15 >nul

REM === STEP 4: Launch Spark job in new terminal and log output ===
echo [4] Starting Spark job with Kafka connector...
start cmd /k "docker-compose exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark-apps/spark_stream.py"

REM === STEP 5: Send a test message to Kafka topic new-scans ===
echo [5] Sending automatic test message to Kafka...
echo {^"scanId^":^"999^", ^"uri^":^"http://auto-message.com/test.jpg^", ^"timestamp^":^"2025-05-14T12:00^", ^"mime^":^"image/png^"} | docker-compose exec -T kafka kafka-console-producer --broker-list kafka:9092 --topic new-scans

REM === STEP 6: Notify user ===
echo [READY] Spark is listening and test message was sent.
echo Check spark_log.txt for output.
pause

