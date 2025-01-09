# ebpms_process

# Project presentation

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

Micro service of enrichment:

Add the fields ABS_ACTIVE_TOWING_COUNTER, ABS_ACTIVE_TOWED_COUNTER, EBS_LOAD, ID_CLIENTE, ID_VEICULO from table FT_CC_POSICAO in table FT_PAYLOAD_EBPMS in DW (Oracle).
It only searches for vehicles that have OF OF SAS_WEB_OPC_BPSPRC in the table manutencao.cliente_obg_financeira (PostgreSQL).
Insert vehicles that have the OF SAS_WEB_OPC_BPSPRC in the VEI_EBPMS_PROCESSAMENTO DW table (Oracle) that is used for processing control by the calculation micro service.
Performs the soft delete of the vehicles that had their weight changed, looking at the DM_VEICULO DW (Oracle).


 	
 

# Technology stack

  As this script is made to be ran as a permanent service grabing real time information, Docker container technology was used to pack and deploy it as a microservice in a container orchestration plataform: Openshift

  
  As openshift demands some monitoring and this script is written in Python, the container main process will be a Flask application with some end-points created for health check purposes (liveness proble and readiness probe)

  Flask app is started using Gunicorn WSGI HTTP Server (https://gunicorn.org/). During the initialization of the Flask app an specific thread named **Thread-ebpms** is started to run the script python_ebpms_join.py. This thread execution is monitored by the **/actuator/health** end-point and halts the application if this the thread is dead.

  The other main component used are:
  
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
    python python_ebpms_join.py  $SERVIDOR_BD_ORACLE_DW_SASCAR_IP $SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN $SERVIDOR_BD_ORACLE_DW_SASCAR_NOME $SERVIDOR_BD_ORACLE_DW_SASCAR_PORTA $SERVIDOR_BD_ORACLE_DW_SASCAR_SENHA $KAFKA_HOSTS $KAFKA_ZOOKEEPER $KAFKA_TOPIC $KAFKA_CONSUMER_GROUP_ID

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
  For validation purposes you can submit the coverage xml file to SASCAR SonarQube(https://sonar.sascar.com.br/dashboard?id=ebpms-process) using your AD login and sonar-scanner utility (https://docs.sonarqube.org/latest/analysis/scan/sonarscanner/):

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
