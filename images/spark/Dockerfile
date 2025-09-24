#+-----------------------------------------+
#|           Imagem Spark 3.4.3            | 
#|                                         |
#| O que decide se é Driver ou Worker      |
#| é o comando de inicialização no compose |
#+-----------------------------------------+
FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

# Layer 1: Install system packages (rarely changes)
RUN apt-get update --fix-missing && \
    apt-get install curl -y && \
    apt-get install openjdk-18-jdk -y && \
    java -version

# Layer 2: Download and install Spark (rarely changes)
RUN curl -O https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz &&\
    tar zxvf spark-3.4.3-bin-hadoop3.tgz -C /usr/local &&\
    rm spark-3.4.3-bin-hadoop3.tgz &&\
    cd /usr/local &&\
    ln -sT spark-3.4.3-bin-hadoop3 spark

# Layer 3: Download AWS SDK Bundle JAR (stable - will be cached)
RUN mkdir -p /usr/local/lib/python3.10/dist-packages/pyspark/jars && \
    curl -L --retry 3 --retry-delay 5 \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    --output /usr/local/lib/python3.10/dist-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar

# Layer 4: Download PostgreSQL JDBC Driver (stable - will be cached)
RUN curl -L --retry 3 --retry-delay 5 \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar \
    --output /usr/local/lib/python3.10/dist-packages/pyspark/jars/postgresql-42.5.0.jar

# Layer 5: Download Hadoop AWS JAR (stable - will be cached)
RUN curl -L --retry 3 --retry-delay 5 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    --output /usr/local/lib/python3.10/dist-packages/pyspark/jars/hadoop-aws-3.3.4.jar

# Layer 6: Copy Hadoop 3.3.4 JARs from Spark installation (stable - will be cached)
RUN cp /usr/local/spark-3.4.3-bin-hadoop3/jars/hadoop-client-api-3.3.4.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/ && \
    cp /usr/local/spark-3.4.3-bin-hadoop3/jars/hadoop-client-runtime-3.3.4.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/ && \
    cp /usr/local/spark-3.4.3-bin-hadoop3/jars/hadoop-yarn-server-web-proxy-3.3.4.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/

# Layer 7: Set up environment variables (frequently changes - put at end)
# Dynamically detect architecture and set JAVA_HOME
RUN ARCH=$(dpkg --print-architecture) && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-18-openjdk-${ARCH}" >> ~/.bashrc && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-18-openjdk-${ARCH}" >> /etc/environment
ENV JAVA_HOME=/usr/lib/jvm/java-18-openjdk-amd64
ENV PATH "${JAVA_HOME}/bin:$PATH"
ENV SPARK_HOME /usr/local/spark
ENV PATH "${SPARK_HOME}/bin:$PATH"

# Layer 8: Set up bashrc (frequently changes - put at end)
RUN echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc && \
    echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc

# Layer 9: Final setup (this is where you'd put frequently changing stuff)
USER root

# If you need to add more environment variables or other frequently changing configs,
# put them here at the end
