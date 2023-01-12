FROM eclipse-temurin:8-jammy

# Download all migration dependencies
RUN mkdir -p /assets/ && cd /assets && \
    curl -OL https://downloads.datastax.com/dsbulk/dsbulk.tar.gz && \
    tar -xzf ./dsbulk.tar.gz && \
    rm ./dsbulk.tar.gz && \
    mv /assets/dsbulk-1.10.0 /assets/dsbulk  && \
    curl -OL https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz && \
    tar -xzf ./cqlsh-astra.tar.gz && \
    rm ./cqlsh-astra.tar.gz && \
    curl -OL https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz && \
    tar -xzf ./spark-2.4.8-bin-hadoop2.7.tgz && \
    rm ./spark-2.4.8-bin-hadoop2.7.tgz

RUN apt-get update && apt-get install -y openssh-server vim python3 --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*  && \
    service ssh start

# Copy CDM jar & template files
ARG MAVEN_VERSION=3.8.6
ARG USER_HOME_DIR="/root"
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
COPY ./src /assets/src
COPY ./pom.xml /assets/pom.xml
COPY ./src/resources/sparkConf.properties /assets/
COPY ./src/resources/partitions.csv /assets/
COPY ./src/resources/primary_key_rows.csv /assets/
COPY ./src/resources/runCommands.txt /assets/

RUN mkdir -p /usr/share/maven /usr/share/maven/ref && \
    curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 && \
    rm -f /tmp/apache-maven.tar.gz && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn && \
    cd /assets && mvn -f ./pom.xml clean package && \
    cp /assets/target/cassandra-data-migrator-*.jar /assets/ && \
    rm -rf /assets/src && \
    rm -rf /assets/target && \
    rm -rf /assets/pom.xml && \
    rm -rf "$MAVEN_HOME" && \
    rm -rf "$USER_HOME_DIR/.m2"

# Add all migration tools to path
ENV PATH="${PATH}:/assets/dsbulk/bin/:/assets/cqlsh-astra/bin/:/assets/spark-2.4.8-bin-hadoop2.7/bin/"

EXPOSE 22

CMD ["/usr/sbin/sshd","-D"]
