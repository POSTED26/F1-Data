FROM apache/spark:latest

# Switch to root to install Python and pip
USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark

# Copy S3 support JARs
COPY hadoop-aws-3.3.1.jar /opt/spark/jars/
COPY aws-java-sdk-bundle-1.11.901.jar /opt/spark/jars/

# Switch back to default Spark user
USER 185
