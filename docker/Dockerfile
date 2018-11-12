FROM vertigomedia/ubuntu-jdk8
USER root

MAINTAINER trecool "358659374@qq.com"

# install mysql
RUN apt-get update
RUN apt-get install -y mysql-client
RUN apt-get install -y mysql-server

# set password and allow remote connections
RUN chown -R mysql:mysql /var/lib/mysql && \
	/etc/init.d/mysql start &&\
	mysqladmin -uroot password 123456 &&\
	mysql -p123456 -e "grant all privileges on *.* to 'root'@'%' identified by '123456';" &&\
	mysql -p123456 -e "grant all privileges on *.* to 'root'@'localhost' identified by '123456';" &&\
	mysql -p123456 -e "CREATE DATABASE test"
RUN sed -i 's/^bind-address.*/bind-address=0.0.0.0/g' /etc/mysql/mysql.conf.d/mysqld.cnf

# install mycat
ADD mycat2.tar.gz /usr/local/
COPY run.sh /usr/local/mycat2/
RUN mkdir /usr/local/mycat2/logs
RUN chmod 777 /usr/local/mycat2/run.sh
EXPOSE 3306
EXPOSE 8066
ENTRYPOINT ["/usr/local/mycat2/run.sh"]
