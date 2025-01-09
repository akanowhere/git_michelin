# ebpms-consume

# Project presentation.

The ebpms project (electronic brake performance monitoring system) is a project that is motivated by UK legislation.
The DVSA (UK traffic department) gives vehicle owners the option to perform 3 out of 4 brake assessments via software service.
More information about: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/517543/EBPMS-industry-standard-specification.pdf

In the data part, the project was divided into three micro services.
Micro service that performs the translation of the binary payload and saves it in the base.
https://git.sascar.com.br/channel/traillers/ebpms-consume
Micro service that performs the enrichment of ebpms data with values ​​from other payloads and also controls the vehicles that must be processed.
https://git.sascar.com.br/channel/traillers/ebpms-process
Micro service that calculates the brake score via algorithm and saves it in a base, made available by Michelin engineer Marc Hammer.
https://git.sascar.com.br/channel/traillers/ebpms-break-health-calculation


# Detailed

Micro service translation of binary payload:

This micro service consumes the topic of kafka sascarBinaryPayloads, to filter only the payloads that contain data for ebpms the first four characters of the payload field must be equal to 01FD. The SoftwareVersion field must also be 41603828 or 41314328.

The payload comes binary and it is necessary to translate the binary string by separating it and converting it correctly as described in the table below:


| CONTENT |           DATA NAME           |                                                                    DESCRIPTION                                                                    |         FORMAT        | START BIT | LENGTH | STOP BIT | FROM… |        …TO | RESOLUTION |  UNIT  |
|:-------:|:-----------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------:|:---------:|:------:|:--------:|-------|-----------:|:----------:|:------:|
|  HEADER | HDR_PAYLOAD_VERSION           | Version of the payload = 0x01                                                                                                                     | integer               | 0         | 8      | 7        | 0     | 255        | 1          | Ø      |
|         | HDR_PAYLOAD_TYPE              | Type of   payload                                                                                                                                 | integer               | 8         | 8      | 15       | 0     | 255        | 1          | Ø      |
|         | HDR_PAYLOAD_CAUSE             | Cause of generation                                                                                                                               | integer               | 16        | 8      | 23       | 0     | 255        | 1          | Ø      |
|         | HDR_GENERATION_NUMBER         | Sequence of   message generation                                                                                                                  | integer               | 24        | 8      | 31       | 0     | 255        | 1          | unity  |
|         | HDR_PAYLOAD_NUMBER            | Number of payloads generated for the sequence                                                                                                     | integer               | 32        | 8      | 39       | 0     | 255        | 1          | unity  |
|         | HDR_MESSAGE_DATE_TIME         | Timestamp of   message generation                                                                                                                 | timestamp             | 40        | 32     | 71       | 0     | 4294967295 | 1          | second |
| CONTENT | FREE_EBPMS_PREFIX             | "EBPMS="                                                                                                                                          | ASCII                 | 72        | 48     | 119      | NA    | NA         | 1          | Ø      |
|         | FREE_EBPMS_RETARDER           | Number of messages with Vehicle Retarder Control Status   active.      Example "A1" = 161 messages with Vehicle Retarder Control Status   active. | ASCII      Hex String | 120       | 16     | 135      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "/"                                                                                                                                               | ASCII                 | 136       | 8      | 143      |       |            |            |        |
|         | FREE_EBPMS_TIME               | Epoch time when brake begins.      Example "5CA4BB10" = 1554299664 for Wednesday 3 April 2019   13:54:24                                          | ASCII      Hex String | 144       | 64     | 207      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "-"                                                                                                                                               | ASCII                 | 208       | 8      | 215      |       |            |            |        |
|         | FREE_EBPMS_DURATION           | Duration of brake event in milliseconds.      Example "120B" = 4619 ms                                                                            | ASCII      Hex String | 216       | 32     | 247      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "/"                                                                                                                                               | ASCII                 | 248       | 8      | 255      |       |            |            |        |
|         | FREE_EBPMS_SPEED_AVERAGE      | Speed average value sent by GPS during the event.      Example "3C" = 60 km/h                                                                     | ASCII      Hex String | 256       | 16     | 271      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "/"                                                                                                                                               | ASCII                 | 272       | 8      | 279      |       |            |            |        |
|         | FREE_EBPMS_ALTITUDE_VARIATION | Sign following by absolute value of altitude variation in   meters.      Example "-1C" = -28 meters.      Example "+0A" = +10 meters.             | ASCII      Hex String | 280       | 24     | 303      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "/"                                                                                                                                               | ASCII                 | 304       | 8      | 311      |       |            |            |        |
|         | FREE_EBPMS_SPEED_BEGIN        | Speed value sent by EBS when brake begins in km/h.      Example "3C" = 60 km/h                                                                    | ASCII      Hex String | 312       | 16     | 327      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "-"                                                                                                                                               | ASCII                 | 328       | 8      | 335      |       |            |            |        |
|         | FREE_EBPMS_SPEED_END          | Speed value sent by EBS when brake ends in km/h.      Example "3C" = 60 km/h                                                                      | ASCII      Hex String | 336       | 16     | 351      | NA    | NA         | 1          | ascii  |
|         | FREE_EBPMS_SEPARATOR          | "/"                                                                                                                                               | ASCII                 | 352       | 8      | 359      |       |            |            |        |
|         | FREE_EBPMS_BRAKE_AVERAGE      | Brake pressure average (mean value between start and end braking)   in kPa      Example "01FF" = 511 kPa                                          | ASCII      Hex String | 360       | 32     | 391      | NA    | NA         | 1          | ascii  |
|  FOOTER | FTR_CHECKSUM                  | Checksum of the payload, calculated on the whole parameters                                                                                       | integer               | 392       | 16     | 407      | 0     | 65535      | 1          | value  |


After the payload is translated and has passed the necessary validations that the code performs, the fields are saved in the FT_PAYLOAD_EBPMS table of the DW (Oracle).


This is the end of the process for this micro service.

Consumer of ebpms payload on FREE128.
 

# Technology stack

  As this script is made to be ran as a permanent service grabing real time information, Docker container technology was used to pack and deploy it as a microservice in a container orchestration plataform: Openshift

  
  As openshift demands some monitoring and this script is written in Python, the container main process will be a Flask application with some end-points created for health check purposes (liveness proble and readiness probe)

  Flask app is started using Gunicorn WSGI HTTP Server (https://gunicorn.org/). During the initialization of the Flask app an specific thread named **Thread-ebpms** is started to run the script python_parser_ebpms.py. This thread execution is monitored by the **/actuator/health** end-point and halts the application if this the thread is dead.

  The other main component used are:
  
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
    python python_parser_ebpms.py  $SERVIDOR_BD_ORACLE_DW_SASCAR_IP $SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN $SERVIDOR_BD_ORACLE_DW_SASCAR_NOME $SERVIDOR_BD_ORACLE_DW_SASCAR_PORTA $SERVIDOR_BD_ORACLE_DW_SASCAR_SENHA $KAFKA_HOSTS $KAFKA_ZOOKEEPER $KAFKA_TOPIC $KAFKA_CONSUMER_GROUP_ID

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
  For validation purposes you can submit the coverage xml file to SASCAR SonarQube(https://sonar.sascar.com.br/dashboard?id=ebpms-consume) using your AD login and sonar-scanner utility (https://docs.sonarqube.org/latest/analysis/scan/sonarscanner/):

  **execute the following command on the root folder of the project**

```shellscript
  sonar-scanner   -Dsonar.projectKey=tpms-d0   -Dsonar.sources=.   -Dsonar.host.url=https://sonar.sascar.com.br   -Dsonar.login=your_token   
```

  **your_token must be created directly on the SonarQube site(https://sonar.sascar.com.br/account/security/)**
  
# Environment information

  For each environment(dev/hom/prd) there is a deployment.env file with database and kafka connection parameters located at **/etc/openshift**:

   * /desenvolvimento
   * /homologacao
   * /master


