from 192.168.137.215:5000/eurlanda/centos-jdk8:1.0

LABEL maintainer="debin.zhu@eurlanda.com"
LABEL name="server"
LABEL description="server"
LABEL release-date="2018-09-03"
LABEL version="1.0"

WORKDIR /
ENV HADOOP_USER_NAME hdfs
RUN mkdir -p /opt/server /opt/server/hadoop_conf /opt/server/config /opt/server/server-logs /opt/hadoop-lib
ADD target/server.jar /opt/server
ADD target/lib /opt/server/lib

RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hadoop-lib

ADD doc/server-compose/start.sh /opt/server/

RUN chmod +x /opt/server/start.sh

CMD sh /opt/server/start.sh