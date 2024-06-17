# Use the official Airflow image as the base
FROM apache/airflow:2.9.0-python3.11

# Switch to root user to install additional packages
USER root

# Install necessary packages including OpenJDK 17
RUN apt-get update && \
    apt-get install -y procps wget gnupg curl openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to the airflow user
USER airflow

# Install the Apache Spark provider
RUN pip install "apache-airflow-providers-apache-spark==3.0.0"

# Set the entrypoint to the Airflow entrypoint
ENTRYPOINT ["/entrypoint"]

# Set default command to scheduler
# CMD ["scheduler"]
