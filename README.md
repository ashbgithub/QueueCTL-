# QueueCTL-
QueueCTL is a CLI-based background job queue system built in Java 17. It supports job enqueueing, worker execution, automatic retries with exponential backoff, and a Dead Letter Queue (DLQ).
Features

Enqueue jobs with shell commands

Multiple worker threads

Automatic retries with exponential backoff

Dead Letter Queue (DLQ) for failed jobs

SQLite persistent storage

Graceful shutdown (Ctrl + C)

Job timeout support (optional)

Setup

Install Java 17 and Maven

Build the project:

mvn clean package


Run the CLI:

java -jar target/queuectl-1.0-SNAPSHOT.jar <command>

Basic Usage
Enqueue a job
java -jar target/queuectl-1.0-SNAPSHOT.jar enqueue '{"id":"job1","command":"echo Hello"}'

Start a worker
java -jar target/queuectl-1.0-SNAPSHOT.jar worker start 1

View job status
java -jar target/queuectl-1.0-SNAPSHOT.jar status

List jobs
java -jar target/queuectl-1.0-SNAPSHOT.jar list
java -jar target/queuectl-1.0-SNAPSHOT.jar list completed
java -jar target/queuectl-1.0-SNAPSHOT.jar list dead

Dead Letter Queue
java -jar target/queuectl-1.0-SNAPSHOT.jar dlq list
java -jar target/queuectl-1.0-SNAPSHOT.jar dlq retry job1
