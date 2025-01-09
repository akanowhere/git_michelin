# ebpms-break-health-calculation

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

Micro calculation service:

This micro service has a processing queue management class. This is necessary because with several pods using the same table to do the same processing, this type of management is necessary in order to avoid duplicate data.
In summary, as the vehicles are inserted in the VEI_EBPMS_PROCESSAMENTO table of the dw (Oracle), the class "locks" a vehicle for each POD using the FOR UPDATE command in the bank select, releasing the vehicle immediately after the calculation process has finished.
The calculation uses the robust linear regression algorithm. The algorithm code was made available by Michelin Engineer Marc Hammer (marc.hammer@michelin.com).
The select that brings the valid records of the vehicle has several filters because only some records are valid for the calculation.
Each value is calculated and made based on two hundred positions, and for the first calculation it is necessary to have two hundred valid positions accumulated in the FT_PAYLOAD_EBPMS table. After this accumulated, each new position validates and a calculation is made accumulating this new position with the other one hundred and ninety-nine previous ones.
The script takes the current information of the maximum weight that the trailer can load from the dispositivo.veiculo  vehicle table (PostGreSql), it is always necessary to get the most current information, so the direct query of the dispositivo.veiculo base.
For each calculation performed, the oldest position used (from the universe of two hundred) is updated, changing the value of the FLAG_CALC column from 0 to 1.
The calculation is done and saved in the FT_EBPMS_CALCULO table of the dw (Oracle)



# Technology stack

  As this script is made to be ran as a permanent service grabing real time information, Docker container technology was used to pack and deploy it as a microservice in a container orchestration plataform: Openshift

  
  As openshift demands some monitoring and this script is written in Python, the container main process will be a Flask application with some end-points created for health check purposes (liveness proble and readiness probe)

  Flask app is started using Gunicorn WSGI HTTP Server (https://gunicorn.org/). During the initialization of the Flask app an specific thread named **Thread-ebpms** is started to run the script python_parser_ebpms.py. This thread execution is monitored by the **/actuator/health** end-point and halts the application if this the thread is dead.

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
    python python_parser_ebpms.py  $SERVIDOR_BD_ORACLE_DW_SASCAR_IP $SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN $SERVIDOR_BD_ORACLE_DW_SASCAR_NOME $SERVIDOR_BD_ORACLE_DW_SASCAR_PORTA 

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

