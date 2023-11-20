FROM ibmjava:8-sdk

RUN apt-get update && apt-get install -y software-properties-common ssh net-tools ca-certificates tar rsync sed

# passwordless ssh
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN mkdir -p /root/.ssh
COPY base/config /root/.ssh/
#RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

RUN wget --no-verbose https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar -xvzf hadoop-2.7.7.tar.gz && \
    mv hadoop-2.7.7 hadoop && \
    rm hadoop-2.7.7.tar.gz && \
    rm -rf hadoop/share/doc/*

RUN ln -s /hadoop/etc/hadoop /etc/hadoop
RUN mkdir -p /logs
RUN mkdir /hadoop-data

ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH /hadoop/bin/:$PATH

COPY base/core-site.xml /etc/hadoop/
COPY base/hdfs-site.xml /etc/hadoop/
COPY base/hadoop-env.sh /etc/hadoop/

ENV HADOOP_CONF_DIR=/etc/hadoop
COPY run.sh /run.sh
RUN chmod a+x run.sh

ENTRYPOINT ["sh", "-c", "service ssh restart; ./run.sh; sleep infinity"]