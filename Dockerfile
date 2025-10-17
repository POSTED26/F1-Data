# Use the official Apache Spark image
FROM apache/spark:latest

# Switch to root to install packages
USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Set a valid home directory to avoid Ivy errors
ENV HOME=/tmp

# Copy Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Pre-bundle Hadoop AWS and AWS SDK JARs for S3 support
COPY hadoop-aws-3.3.2.jar /opt/spark/jars/
COPY aws-java-sdk-bundle-1.12.550.jar /opt/spark/jars/


# Set working directory
WORKDIR /app

# Copy your Spark job code
COPY . /app
