#!/bin/bash

# Variables
# FLINK_CONTAINER="flink_jobmanager"
# JOB_JAR_PATH="./kafka/consumer/target/FlinkCrypto-1.0-SNAPSHOT.jar"
# JOB_JAR_NAME="job.jar"

# # Copy the JAR to the Flink container
# echo "Copying JAR to Flink container..."
# docker cp $JOB_JAR_PATH $FLINK_CONTAINER:/$JOB_JAR_NAME

# # Submit the job
# echo "Submitting the job to Flink..."
# docker exec -it $FLINK_CONTAINER flink run /$JOB_JAR_NAME

# # Done
# echo "Job submitted successfully. Check the Flink Web UI at http://localhost:8081."

# ./../flink-1.18.1/bin/stop-cluster.sh
# ./../flink-1.18.1/bin/start-cluster.sh
# ./../flink-1.18.1/bin/stop-cluster.sh
# ./../flink-1.18.1/bin/flink run ./kafka/consumer/target/FlinkCrypto-1.0-SNAPSHOT.jar


#!/bin/bash

# Variables
FLINK_DIR="./../flink-1.18.1"
JOB_JAR_PATH="./kafka/consumer/target/FlinkCrypto-1.0-SNAPSHOT.jar"

# Ensure the Flink directory exists
if [ ! -d "$FLINK_DIR" ]; then
  echo "Error: Flink directory not found at $FLINK_DIR"
  exit 1
fi

# Ensure the job JAR exists
if [ ! -f "$JOB_JAR_PATH" ]; then
  echo "Error: Job JAR not found at $JOB_JAR_PATH"
  exit 1
fi

# Stop the Flink cluster (if running)
echo "Stopping any running Flink cluster..."
$FLINK_DIR/bin/stop-cluster.sh
sleep 5

# Start the Flink cluster
echo "Starting Flink cluster..."
$FLINK_DIR/bin/start-cluster.sh

# Give the cluster time to initialize
echo "Waiting for Flink cluster to initialize..."
sleep 5

# Submit the job
echo "Submitting the job to Flink..."
$FLINK_DIR/bin/flink run $JOB_JAR_PATH

# Optional: Stop the Flink cluster after the job completes
# Uncomment the following lines if you want to stop the cluster automatically
# echo "Stopping Flink cluster..."
# $FLINK_DIR/bin/stop-cluster.sh

# Done
echo "Job submitted successfully. Check the Flink Web UI at http://localhost:8081."

