FROM python:3.8.16
# WORKDIR /home/dbsgh3322
USER root
RUN apt-get update
RUN apt-get install vim -y 
# COPY .env /root/airflow/
# doesn't work below export command. so export inside the container
# RUN export $(cat /root/airflow/.env | xargs)
# ENV $(cat .env | xargs)
COPY env_setting.txt ./
COPY requirements.txt ./
RUN pip install -r requirements.txt
# RUN mkdir -p ~/airflow/plugins
# CMD [ "export $(cat /root/airflow/.env | xargs)" ]