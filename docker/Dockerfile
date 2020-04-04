FROM centos:centos7.5.1804

RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup
COPY ./docker/CentOS7-Base-163.repo /etc/yum.repos.d/

RUN yum -y update
RUN yum install vim -y \
    && yum install net-tools -y \
    && yum install bind-utils -y

COPY mysql-api-server /opt/
COPY conf /opt/conf

ENTRYPOINT cd /opt/ && ./msyql-api-server --listPort=6103
