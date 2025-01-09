# The first instruction is what image we want to base our container on
# We Use an official Python runtime as a parent image

FROM registry.access.redhat.com/rhscl/python-36-rhel7

USER root

ARG release=19
ARG update=5
RUN wget https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm -O /tmp/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm && \
    yum -y install /tmp/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm && \
    yum clean all -y && \
    rm -rfv /tmp/*


# Install Python key libraries
RUN pip install --upgrade pip && pip3 install cx_Oracle && pip install psycopg2-binary
# The enviroment variable ensures that the python output is set straight
# to the terminal without buffering it first
ENV PYTHONUNBUFFERED 1

# create root directory for our project in the container
RUN mkdir /streaming_ebpms_calc

# Set the working directory
WORKDIR /streaming_ebpms_calc

# Copy the current directory contents into the container 

ADD . /streaming_ebpms_calc


RUN pip3 install -r requirements.txt

EXPOSE 8080

#CMD gunicorn --bind 0.0.0.0:8080 --log-level=debug app:app
CMD exec python3 app.py
