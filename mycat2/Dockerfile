# 镜像
FROM mysql:8.0.16

# 作者
MAINTAINER zwy <1019100252@qq.com>


RUN buildDeps='procps wget' \
    && apt-get update \
    && apt-get install -y $buildDeps
#    && apt-get purge -y --auto-remove $buildDeps


# 配置MySql
ENV MYSQL_DATABASE sw-docker
ENV MYSQL_ROOT_PASSWORD 123456
ENV MYSQL_ROOT_HOST '%'

# 定义会被容器自动执行的目录
ENV AUTO_RUN_DIR ./docker-entrypoint-initdb.d

# 定义初始化sql文件
ENV INIT_SQL *.sql

# 下载初始化数据库
RUN wget -P  $AUTO_RUN_DIR/ https://gitee.com/zwy_qz/mycatrelease/raw/master/sql/db1.sql  \
   &&  wget -P  $AUTO_RUN_DIR/ https://gitee.com/zwy_qz/mycatrelease/raw/master/sql/db2.sql  \
   &&  wget -P  $AUTO_RUN_DIR/ https://gitee.com/zwy_qz/mycatrelease/raw/master/sql/db2.sql


# 把要执行的sql文件放到/docker-entrypoint-initdb.d/目录下，容器会自动执行这个sql
#COPY ./$INIT_SQL $AUTO_RUN_DIR/

# 给执行文件增加可执行权限
RUN chmod a+x $AUTO_RUN_DIR/$INIT_SQL

#https://download.java.net/openjdk/jdk8u41/ri/openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz
#https://github-production-release-asset-2e65be.s3.amazonaws.com/99313507/70323e80-84a6-11ea-8dbc-b4a5bd5fcca8?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A%2F20200425%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200425T124146Z&X-Amz-Expires=300&X-Amz-Signature=175e164ce27dd6177d353eada6ed8d94841c7e5fa66b7aed8a2598c488fbe052&X-Amz-SignedHeaders=host&actor_id=0&repo_id=99313507&response-content-disposition=attachment%3B%20filename%3Dmycat2-1.03-SNAPSHOT.tar.gz&response-content-type=application%2Foctet-stream

RUN wget "https://github.com/MyCATApache/Mycat2/releases/download/2020-4-20/mycat2-1.03-SNAPSHOT.tar.gz" \
    && wget 'https://download.java.net/openjdk/jdk8u41/ri/openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz' \
    && tar -xvf mycat2-1.03-SNAPSHOT.tar.gz -C /opt \
    && tar -zxvf openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz -C /opt \
    && chmod a+x /opt/mycat/bin/* \
    && chmod a+x /opt/java-se-8u41-ri/bin/*

ENV JAVA_HOME /opt/java-se-8u41-ri
ENV PATH $JAVA_HOME/bin:$PATH

#RUN echo "/opt/mycat/bin//mycat start && ./entrypoint.sh" > /opt/init.sh \
#    &&  chmod a+x /opt/init.sh

RUN echo  '#!/bin/bash ' >>   /opt/init.sh \
   && echo  "echo 'hello'" >>   /opt/init.sh \
   && echo  "/opt/mycat/bin//mycat start &" >>   /opt/init.sh \
   && echo  "docker-entrypoint.sh mysqld" >>   /opt/init.sh  \
   &&  chmod a+x /opt/init.sh

VOLUME ["/opt/mycat2/mycat"]


ENV PATH /opt/:$PATH

Entrypoint ["init.sh"]

# 暴露端口
EXPOSE 3306 8066 1984