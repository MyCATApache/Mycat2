#FROM openjdk:8-jdk-alpine
FROM java:8-jdk
MAINTAINER gaozhiwen <gaulzhw@gmail.com>

EXPOSE 8066
EXPOSE 9066

COPY mycat2*linux.tar.gz /root/mycat2.tar.gz
RUN tar -zxvf /root/mycat2.tar.gz -C /root \
    && rm -rf /root/mycat2.tar.gz \
    && mkdir -p /root/mycat2/logs

ENV MYCAT_HOME "/root/mycat2"
ENTRYPOINT [ "/root/mycat2/bin/mycat", "console" ]

#配置文件路径：/root/mycat2/conf
