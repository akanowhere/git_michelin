# positions_consume

Main Kafka consumer for traillers positions in real time!

# Objective

Provide position Data to TPMS-D0 reports and healthcheck.
Provide position payload details in a structured way for Trip, ebpms and daily positions reports

# TPMS-D0

Get Tyres temperature and pressure from all valid and active sensors per position from kafka Topic (topicSummaryDevice_incidente) and store it on the Oracle DW database at FT_CC_TEMP_PRES_PNEU_DETALHADO table.

# Rules

 1. All the messages captured from **topicSummaryDevice_incidente** must have their dates verified:

   * 1.1 Positions with future dates (Date > D-0) are ignored.

   * 1.2 Memory positions are considered in this process as well

 3. Once the date of message is validated few other verifications must be done before grabing the max temperature and min pressure from tyres:

   * 3.1 Device serial can not be null and should be higher than 0
   * 3.2 Check if there is any sensor information
   * 3.3 Only valid sensors are considered using the following criteria:
      
      * 3.3.1 The 'status' should be 'activated'; 
      * 3.3.2 The 'com' should be 2; 
      * 3.3.3 The 'identifier' must be higher than 0 and smaller than 4294967293.

   * 3.4 Check temperature and pressure from all sensors and grab the highest temperature and lowest pressure values:

     * 3.4.1 Update the db table when the highest temperature value is higher than the current one stored in the aggregated database table
     * 3.4.2 Update the db table when the lowest pressure value lower than the current one stored in the aggregated database table
     * 3.4.3 Store detailed information (temperature and pressure) per tyre / sensor into db detailed table

# Trailler positions Data

  CRCU Equipment sends different kinds of data split in different payloads (eg. DIAG, TPM, EBS, WSN)

  Every new position is processed in a way that the relevant data is extracted and stored into FT_CC_POSICAO table at SASCAR_DW. 

# Technology stack

  As this script is made to be ran as a permanent service grabing real time information, Docker container technology was used to pack and deploy it as a microservice in a container orchestration plataform: Openshift

  Currently there is a dev environment running at SASCAR infrastructure: https://opc-dev.sascar.com.br:8443/console/project/streaming-tpms-agg-dev

  As openshift demands some monitoring and this script is written in Python, the container main process will be a Flask application with some end-points created for health check purposes (liveness proble and readiness probe)

  Flask app is started using Gunicorn WSGI HTTP Server (https://gunicorn.org/). During the initialization of the Flask app an specific thread named **Thread-tpms** is started to run the script python_con_cc_rep_temp_pres_d0.py. This thread execution is monitored by the **/actuator/health** end-point and halts the application if this the thread is dead.

  The other main components used are:
  
  pyKafka: Used to connect and consume Kafka messages
  cx_Oracle: Oracle connection component which uses native oracle drivers to connect to Oracle instance

# Setting up local dev environment

  As a good practice when coding in Python it's highly recommended the creation of a virtual environment: 
  
```shellscript   
  python3 -m venv nameofyourenvironment

```

  Once you have your own virtualenvironment created just run the following command in order to install  all the Python dependencies on it:

```shellscript   
  pip install -r requirements.txt

```

  To Script itself can be called directly as follows:

  ```python
    python python_con_cc_rep_temp_pres_d0.py  $SERVIDOR_BD_ORACLE_DW_SASCAR_IP $SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN $SERVIDOR_BD_ORACLE_DW_SASCAR_NOME $SERVIDOR_BD_ORACLE_DW_SASCAR_PORTA $SERVIDOR_BD_ORACLE_DW_SASCAR_SENHA $KAFKA_HOSTS $KAFKA_ZOOKEEPER $KAFKA_TOPIC $KAFKA_CONSUMER_GROUP_ID

  ```

  As an optional step you might want to test the script locally running it in a docker container simulating the real environment. For that purpose you must install Docker and Docker Compose tool in order to execute existing **docker-compose.yaml** with the proper environment variables. (https://docs.docker.com/compose/install/)

  The basic commands build and run it locally are:

```shellscript

  docker-compose build

  docker-compose up
      
```
  Don't forget to run the **build** command before **up** in order to reflect your latest code changes into the Docker image.

# Unit Tests

  After any code change make sure that all the unit tests are passing and the code coverage is equal or higher than **95%**. Don't forget to update or write new tests when necessary.

  To run the unit tests locally and check code coverage make sure you coverage installed in your virtual environment...

```shellscript
  pip install coverage

```
  ...and execute the following commands:
```shellscript
  coverage run -m unittest discover -p "*.py"

  coverage report

  coverage xml

```
  For validation purposes you can submit the coverage xml file to SASCAR SonarQube(https://sonar.sascar.com.br/dashboard?id=tpms-d0) using your AD login and sonar-scanner utility (https://docs.sonarqube.org/latest/analysis/scan/sonarscanner/):

  **execute the following command on the root folder of the project**

```shellscript
  sonar-scanner   -Dsonar.projectKey=tpms-d0   -Dsonar.sources=.   -Dsonar.host.url=https://sonar.sascar.com.br   -Dsonar.login=your_token   
```

  **your_token must be created directly on the SonarQube site(https://sonar.sascar.com.br/account/security/)**
  
# Environment information

  For each environment(dev/hom/prd) there is a deployment.env file with database and kafka connection parameters located at **/etc/openshift**:

   * /streaming-tpms-agg-dev
   * /streaming-tpms-agg-hom
   * /streaming-tpms-agg-prd
