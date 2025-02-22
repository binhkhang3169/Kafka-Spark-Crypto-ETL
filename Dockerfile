FROM apache/spark:3.1.2

# Install Java and necessary packages
RUN apt-get update && apt-get install -y openjdk-8-jdk wget curl

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -O hadoop.tar.gz && \
    tar -xzf hadoop.tar.gz -C /opt && \
    rm hadoop.tar.gz

# Set HADOOP_HOME
ENV HADOOP_HOME=/opt/hadoop-3.3.1
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install Python and necessary packages
RUN apt-get install -y python3 python3-pip
RUN pip3 install pyspark[sql,kafka]

# Set Python 3 as default
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3

# Copy the script into the container
COPY scripts/consumer.py /app/scripts/consumer.py