# base is common to all build containers
FROM eclipse-temurin:8-jammy AS base
RUN apt-get update && \
    mkdir -p /assets

# This build container compiles the CDM .jar file; ultimately we should just pull
# from Maven/similar into the client container
FROM base AS build-jar
ARG MAVEN_VERSION=3.8.6
ARG USER_HOME_DIR="/root"
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
COPY ./src /assets/src
COPY ./pom.xml /assets/pom.xml

RUN mkdir -p /usr/share/maven /usr/share/maven/ref /assets && \
    curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn && \
    cd /assets && mvn -f ./pom.xml clean package 

# This build container contains executables for the Cassandra Data Migrator 
FROM base AS build-client
RUN cd /assets && \
    curl -OL https://downloads.datastax.com/dsbulk/dsbulk.tar.gz && \
    tar -xzf ./dsbulk.tar.gz && \
    mv /assets/dsbulk-1.10.0 /assets/dsbulk

RUN cd /assets && \
    curl -OL https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz && \
    tar -xzf ./cqlsh-astra.tar.gz

RUN cd /assets && \
    curl -OL https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz && \
    tar -xzf ./spark-2.4.8-bin-hadoop2.7.tgz 

# This is the final runnable container
FROM base 
RUN apt-get install -y vim python3 --no-install-recommends && \
    rm -rf /var/lib/apt/lists/* 
COPY ./src/resources/sparkConf.properties /assets/
COPY ./src/resources/partitions.csv /assets/
COPY ./src/resources/primary_key_rows.csv /assets/
COPY ./src/resources/runCommands.txt /assets/
COPY --from=build-jar /assets/target/cassandra-data-migrator-*.jar /assets/
COPY --from=build-client /assets/dsbulk /assets/dsbulk
COPY --from=build-client /assets/cqlsh-astra /assets/cqlsh-astra
COPY --from=build-client /assets/spark-2.4.8-bin-hadoop2.7 /assets/spark-2.4.8-bin-hadoop2.7
COPY ./run.sh /

# Add all migration tools to path
ENV PATH="${PATH}:/assets/dsbulk/bin/:/assets/cqlsh-astra/bin/:/assets/spark-2.4.8-bin-hadoop2.7/bin/"

CMD ["/bin/bash","/run.sh"]
