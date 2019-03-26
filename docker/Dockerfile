FROM docker.apple.com/cpbase/cp-base:1.8.33

MAINTAINER  PIE-Cassandra <pie-cassandra@group.apple.com>

COPY yum.repos.d/ /etc/yum.repos.d/
COPY install-java-bin /usr/local/bin/

ENV JAVA_HOME /usr/local/java/jdk1.8.0_152/

# install jdk8 for ccm
RUN yum -y install isg-jdk-180152-1.0-1.x86_64

ENV ANT_VERSION=1.10.3 \
    JAVA_TOOLS_DIR=/opt \
    MAVEN_VERSION=3.3.9

RUN cd $JAVA_TOOLS_DIR && \
  install-java-bin ant https://artifacts.apple.com/apache-dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.zip

RUN cd $JAVA_TOOLS_DIR && \
  install-java-bin mvn https://artifacts.apple.com/apache-dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.zip