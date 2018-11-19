FROM alpine:3.8

MAINTAINER trezhang 358659374@qq.com

# install jre and mysql
RUN apk update && \
	apk add openrc --no-cache && \
	apk add openjdk8 && \
	apk add mysql && \
	apk add mysql-client && \
	rm -rf /var/cache/apk/* && \
	cp /usr/share/mariadb/mysql.server /etc/init.d/mysqld
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
	
# install glibc
# https://github.com/sgerrand/alpine-pkg-glibc
RUN apk --no-cache add ca-certificates wget && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
	wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.28-r0/glibc-2.28-r0.apk && \
	apk add glibc-2.28-r0.apk
	
# install mycat service
RUN apk add git && \
	apk add maven && \
	git clone https://github.com/MyCATApache/Mycat2.git && \
	cd Mycat2/source && \
	mvn package -DskipTests && \
	cd target && \
	tar -zxvf mycat2*-linux.tar.gz && \
	mkdir /usr/local/mycat2 && \
	mv mycat2 /usr/local
COPY mycat_conf/* /usr/local/mycat2/conf
RUN mkdir /usr/local/mycat2/logs

EXPOSE 3306
EXPOSE 8066

COPY run.sh /
COPY init_db.sh /
RUN chmod 777 /run.sh
RUN chmod 777 /init_db.sh
CMD ["/run.sh"]