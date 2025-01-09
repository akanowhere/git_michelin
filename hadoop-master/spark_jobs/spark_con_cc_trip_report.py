# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 17:08:53 2018

@author: carlos.santanna.ext
"""

### Import libraries ###
from __future__ import print_function, division
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, udf, lit, sum, when, min, max
from pyspark.sql.types import *
import sys
from datetime import datetime, timedelta
import config_database as dbconf
import config_aws as awsconf


### TO DO ###
 # 1) Contemplar a tabela com as viagens em aberto do dia anterior
 # 2) O DateLimit para o "Loss of message" dever ser a data que vem do parâmetro da chamada do script
 # OBS: As alterações acima serão feitas apenas quando o trip algorithm for executado nos moldes do data lake (GMT + Memória)

def getTruckId(ofTruckId, truckidArray):
    # Check Financial obligation for TRUCKID
    if (len(str(ofTruckId)) > 0):
        
        if(truckidArray is not None):
           truck = truckidArray[0]
          
           if (truck["status"] == 'active'):
              return long(truck["id"])
        
        else:
           return long(0)
    
    else:
        if(truckidArray is not None):
            for truck in truckidArray:
                if (truck["status"] == 'active'):
                   return long(truck["id"])
            
            return long(0)
    
    return long(0)


def getTruckAssetsIds(truckidArray):
    # Return all the valid truck and assets IDs
    truckAssetList = []
    
    if(truckidArray is not None):
       for truck in truckidArray:
           truckAssetList.append(truck["id"]) 
            
    return str(truckAssetList)

def getTripType(motion):
    
    '''
    TRIP TYPE
      1 - POWERED
      2 - BATTERY
      3 - MULTIMODAL
      4 - UNDEFINED (ABNORMAL)
    '''
    
    if(motion == 2): # Main power
       return int(1)
       
    elif(motion == 3): # Battery
       return int(2)
       
    elif(motion == 4): #Modal
       return int(3)
       
    elif(motion == 5): #Abnormal
       return int(4)
       
    else:
       return int(4)
       

def checkTripStart(payloadCause):
    # Check if it is a trip start #
    if (payloadCause == 4):
        return 1
    else:
        return 0


def checkTripEndSecondRule(canActivity, powerSource, tripCanActivity, tripPowerSource):
    # Rule #2 - Check the power source switch and the ebs source (can_activity)
    
    # Check if the power source was switched
    if (powerSource != tripPowerSource):
       
       # In this case the power source was switched from battery to main
       if (tripPowerSource == 0):
          
          # Check if the can activity was not detected and the the current is detected
          if ((tripCanActivity != 2) and (canActivity == 2)):
             return 0
         
          elif ((tripCanActivity == 0) and (canActivity == 0)):
             return 1
         
          else:
             return 1
             
       # In this case the power source was switched from main to battery
       else:
          # Check if the can activity was not detected and the the current is detected
          if ((tripCanActivity == 2) and (canActivity != 2)):
             return 0
         
          elif ((tripCanActivity == 0) and (canActivity == 0)):
             return 1
         
          else:
             return 1
    else:
       return 1


def checkTripEndThirdRule(positionDate, nextPositionDate, positionTimeDiff):
    #Rule #3 - Check if the difference of the current position date and the last one is higher than 15 minutes
    
    diff = nextPositionDate - positionDate
    diff_minutes = float(diff.seconds/60)
    
    if (diff_minutes >= float(positionTimeDiff)):
        return 0
    else:
        return 1


def checkTripEndFourthRule(motion, tripMotionReference):
    # Rule #4 - Check the change of the motion state of the trip
    if (tripMotionReference == 3 and motion == 4):
        return 0
    
    if(tripMotionReference == 5 and motion == 4):
       return 0
    
    return 1


def checkTripEndFifthRule(gnss, nextGnss):
    # Rule #5 - Check if the GNSS will be reseted in the next position
    if ((gnss > 0) and (nextGnss < gnss)):
       return 0
    else:
       return 1


def checkTripEndSixthRule(payloadCause):
    # Rule #6 - Check for a SAFETY message sent by the equipment
    if (payloadCause == 11):
        return 0
    else:
        return 1


def getKmRodados(odometro, nextOdometro):
    # Get the traveled distance in 'KM' from the current position and the next one
    
    if(nextOdometro > 0):
       distance = round(float((nextOdometro - odometro)/10), 1)
       
    else:
       distance = float(0)
    
    return distance


def getTripPositionTimeDiff(tripPowerSorce):
    # Get the maximum position time diff according to the power sorce of the trip
    if (tripPowerSorce == 1):
       return 45
    else:
       return 15


def getUnmotionPositionTimeDiff(unmotionPositionDate, firstUnmotionPositionDate):
    #Rule #3 - Get the difference of the current position date and the first unmotion position date
    
    diff = unmotionPositionDate - firstUnmotionPositionDate
    diff_minutes = float(diff.seconds/60)
    
    return diff_minutes


def getTripTime(tripDtBegin, tripDtEnd):
    # Get the trip time in minutes
    
    diff = tripDtEnd - tripDtBegin
    diff_minutes = round(float(diff.seconds/60), 2)
    
    return diff_minutes


def getTripSpeedAverage(tripDistance, tripTime):
    # Get the speed average of the trip
    
    #Convert to hours
    tripTimeHours = round(tripTime/60, 2)
    
    if(tripTimeHours > float(0) and tripDistance > float(0)):
       speedAverage = round(float(tripDistance/tripTimeHours), 2)
    
       return speedAverage
    
    else:
       return float(0)


def getPositionTempoMinuto(posDatahora, nextPosDatahora, posDatahoraAnt, inicioDia):
    # Get the position time in minutes
    
    if(posDatahoraAnt == inicioDia):
       diff = nextPosDatahora - inicioDia
       
    else:    
       diff = nextPosDatahora - posDatahora
       
    tempoMinutoPosicao = round(float(diff.seconds/60), 4)
    
    return tempoMinutoPosicao


def getTripDistance(beginGnss, endGnss):
    # Get the distance of the trip in km
    
    distance = float(round((endGnss - beginGnss)/1000,2))
    
    return distance
    

def processVehicleTrip(group):
    # Declare and initiate the function global variables #
    vehicleId = 0
    isVehicleInTrip = 0
    vehicleTrip = 0
    previousPositionDate = ''
    previousGnssDistance = 0
    previousKnownLatitude = ''
    previousKnownLongitude = ''
    tripStartPowerSource = 0
    tripStartCanActivity = 0
    tripUnmotionTimeDiff = 0
    tripDateBegin = ''
    tripDateEnd = ''
    tripMotion = 0
    #isFirstTripPosition = 0
    tripBeginLatitude = 0
    tripBeginLongitude = 0
    tripEndLatitude = 0
    tripEndLongitude = 0
    kmRodado = 0
    idEpisodio = 1
    #tempoMinutoPosicao = 0
    nextPositionDate = ''
    nextPositionOdometro = 0
    isVehicleUnmotion = 0
    tripEndCause = ''
    
    rowList = []
    
    # Variable to control the max time interval of unmotion positions after a trip. 
    # Current is fixed to 10 minutes
    tripUnmotionTimeInterval = 5
    
    # Varible to keep the first datetime of an unmotion position during a trip. 
    # This will be used to control the end trip rule #1
    tripFirstUnmotionPositionDate = ''
    
    # Variable to control the max time interval between 2 (two) positions during a trip. 
    # Current the value depends of the power source of the trip. For internal the value
    # is 15 minutes, and for external (main power) is 45 minutes
    tripPositionTimeDiff = 0
    
    # Variable to keep the mode of the trip. Possible values are: '0' for undifined, '1' for normal and '2' for modal
    tripStartTransportMode = 0
    
    # Variable to control if it is the last positions processed for tge current vehicle
    isLastVehiclePosition = 0
    
    # Get row key/values. In this case, both key and values is a tuble #
    vehicleId, groupValues = group[0], group[1]
    
    # Variable to control the lack of diff of the odometer when the vehicle finished the day before on a trip
    diffLastInfoKmRodados = float(0)
    
    # Loop over the array to read position by position #
    #for position in groupValues:
    for i in range(len(groupValues)):
        position = groupValues[i]
        rowItems = {}
        
        # Get the values from the current position #
        currentTimeGMT0 = position[0]
        currentDataDirecao = position[1]
        currentDataHora = position[2]
        currentIgnicao = position[3]
        currentVelocidade = position[4]
        currentLatitude = position[5]
        currentLongitude = position[6]
        currentFinalDia = position[7]
        currentInicioDia = position[8]
        currentGpsValido = position[9]
        currentBloqueio = position[10]
        currentBateria = position[11]
        currentMemoria = position[12]
        currentIButon = position[13]
        currentHorimetro = position[14]
        currentIdMotorista = position[15]
        currentNomeMotorista = position[16]
        currentIdEvento = position[17]
        currentBateriaInterna = position[18]
        currentLanterna = position[19]
        currentTempoEvento = position[20]
        currentMotion = position[21]
        currentPayloadCause = position[22]
        currentPowerSource = position[23]
        currentGnssDistance = position[24]
        currentCanActivity = position[25]
        currentVehicleOdometro = position[26]
        currentPeso = position[27]
        currentTruckId = position[28]
        
        # Fields to control the last status info of the vehicle fot the day before
        lastInfoPositionDate = position[29]
        lastInfoOdometro = position[30]
        lastInfoStopMove = position[31]
        
        currentTruckAssetsIds = position[32]
        currentSoftwareVersion = position[33]
        
        # Check last status info of the vehicle if it is the first position of the current day
        if (i == 0):
            #lastInfoPositionDate = datetime.strptime(lastInfoPositionDate, "%Y-%m-%d %H:%M:%S")
            
            # Check if the vehicle was on a trip based on the last info postision
            if((lastInfoStopMove is not None) and (lastInfoStopMove == 1)):
               # Get the max diff between dates based on the power source
               tripPositionTimeDiff = getTripPositionTimeDiff(currentPowerSource)
               
               # Check if the trip has ended based on position date diff condition
               isTripEnded = checkTripEndThirdRule(lastInfoPositionDate, currentTimeGMT0, tripPositionTimeDiff)
               
               if(isTripEnded == 1):
                  currentPayloadCause = 4
                  idEpisodio = 1
                  diffLastInfoKmRodados = getKmRodados(lastInfoOdometro, currentVehicleOdometro)
        else:
            diffLastInfoKmRodados = 0
        
        if (isVehicleInTrip == 0):
            #check for trip start
            isVehicleInTrip = checkTripStart(currentPayloadCause)
            
            if (isVehicleInTrip == 1):
                vehicleTrip = 1
                tripStartPowerSource = currentPowerSource
                tripStartCanActivity = currentCanActivity
                tripPositionTimeDiff = getTripPositionTimeDiff(tripStartPowerSource)
                tripUnmotionTimeDiff = 0
                tripMotion = currentMotion
                tripFirstUnmotionPositionDate = ''
                tripDateBegin = currentTimeGMT0
                tripBeginLatitude = currentLatitude
                tripBeginLongitude = currentLongitude
                tripStartTransportMode = getTripType(currentMotion)
                
                # Check if it is the first position of the vehicle. In it is, then we do not increment the value of the episode, since it is the first one
                # If it is not the first position, that means the first episode was a stop and now it is a move, so in this case we should incremente the variable value
                if(i > 0):
                  idEpisodio = idEpisodio + 1
        
        if(isVehicleInTrip == 1):
            ### Check for trip end by evaluating all the possible conditions ###
            # Rule #1
            
            # Check if the vehicle was not in unmotion condition and the current status is unmotion
            if(isVehicleUnmotion == 0 and currentPayloadCause == 5):
               isVehicleInTrip = 0
               
            # Check if the vehicle was already in unmotion condition and the current status is anything but 4 (motion)
            elif(isVehicleUnmotion == 1 and currentPayloadCause != 4):
               isVehicleInTrip = 0
            
            # Check if the vehicle was already in unmotion condition and the current status is 4 (motion)
            elif(isVehicleUnmotion == 1 and currentPayloadCause == 4):
                isVehicleUnmotion = 0
                isVehicleInTrip = 1
                tripFirstUnmotionPositionDate = ''
            
            # Check if the current position is a possible end trip
            if (isVehicleInTrip == 0):
                if (tripFirstUnmotionPositionDate == ''): # Remain on trip
                   tripFirstUnmotionPositionDate = currentTimeGMT0
                
                isVehicleUnmotion = 1
                
                # Check if the diffence of the first unmotion position to the current position is higher then the 'tripUnmotionTimeInterval'
                tripUnmotionTimeDiff = getUnmotionPositionTimeDiff(currentTimeGMT0, tripFirstUnmotionPositionDate)
                
                if (tripUnmotionTimeDiff >= float(tripUnmotionTimeInterval)):
                   tripUnmotionTimeDiff = 0
                   isVehicleUnmotion = 0
                   tripFirstUnmotionPositionDate = ''
                   tripDateEnd = currentTimeGMT0
                   
                   if(currentLatitude != 0):
                     tripEndLatitude = currentLatitude
                   else:
                     tripEndLatitude = previousKnownLatitude
                     
                   if(currentLongitude != 0):
                     tripEndLongitude = currentLongitude
                   else:
                     tripEndLongitude = previousKnownLongitude
                   
                   tripEndCause = 'UNMOTION'
                    
                else:
                    isVehicleInTrip = 1
               
            if (isVehicleInTrip == 1):
               # Rule #5
               try:
                  # Get the next position
                  nextPosition = groupValues[i+1]
                  
                  #Get the GNSS of the next position
                  nextPositionGnssDistance = nextPosition[24]
                  
                  # Check if the current position is an end trip
                  isVehicleInTrip = checkTripEndFifthRule(currentGnssDistance, nextPositionGnssDistance)
                  
                  if (isVehicleInTrip == 0):
                     tripDateEnd = currentTimeGMT0
                     
                     if(currentLatitude != 0):
                       tripEndLatitude = currentLatitude
                     else:
                       tripEndLatitude = previousKnownLatitude
                       
                     if(currentLongitude != 0):
                       tripEndLongitude = currentLongitude
                     else:
                       tripEndLongitude = previousKnownLongitude
                       
                     tripEndCause = 'GNSS DISTANCE TO ZERO'
                     
               except:
                   #remain on trip
                   isVehicleInTrip = 1
            
            if (isVehicleInTrip == 1):
               # Rule #3
               try:
                  #Get next position
                  nextPosition = groupValues[i+1]
                  
                  #Get the next position date
                  nextPositionDate = nextPosition[0]
                  
                  # Check if the current position is an end trip
                  isVehicleInTrip = checkTripEndThirdRule(currentTimeGMT0, nextPositionDate, tripPositionTimeDiff)
                  
                  if (isVehicleInTrip == 0):
                     
                     tripDateEnd = currentTimeGMT0
                     
                     if(currentLatitude != 0):
                       tripEndLatitude = currentLatitude
                     else:
                       tripEndLatitude = previousKnownLatitude
                       
                     if(currentLongitude != 0):
                       tripEndLongitude = currentLongitude
                     else:
                       tripEndLongitude = previousKnownLongitude
                     
                     tripEndCause = 'LACK OF MESSAGES'
                     
               except:
                     isVehicleInTrip = 0
                     
                     #tripDateEnd = currentTimeGMT0 + timedelta(minutes = 15)
                     tripDateEnd = currentTimeGMT0
                   
                     if(currentLatitude != 0):
                        tripEndLatitude = currentLatitude
                     else:
                        tripEndLatitude = previousKnownLatitude
                     
                     if(currentLongitude != 0):
                        tripEndLongitude = currentLongitude
                     else:
                        tripEndLongitude = previousKnownLongitude
                     
                     tripEndCause = 'LOSS OF MESSAGES'            
            
            if (isVehicleInTrip == 1):
               # Rule #4
               
               # Check if the current position is an end trip
               isVehicleInTrip = checkTripEndFourthRule(currentMotion, tripMotion)
               
               if (isVehicleInTrip == 0):
                  tripDateEnd = currentTimeGMT0
                  
                  if(currentLatitude != 0):
                    tripEndLatitude = currentLatitude
                  else:
                    tripEndLatitude = previousKnownLatitude
                    
                  if(currentLongitude != 0):
                    tripEndLongitude = currentLongitude
                  else:
                    tripEndLongitude = previousKnownLongitude
                  
                  tripEndCause = 'TRANSPORT MODE CHANGE'
            
            
            if (isVehicleInTrip == 1):
               # Rule #5
               
               # Check if the current position is an end trip
               isVehicleInTrip = checkTripEndSixthRule(currentPayloadCause)
               
               if (isVehicleInTrip == 0):
                  tripDateEnd = currentTimeGMT0
                  
                  if(currentLatitude != 0):
                    tripEndLatitude = currentLatitude
                  else:
                    tripEndLatitude = previousKnownLatitude
                   
                  if(currentLongitude != 0):
                    tripEndLongitude = currentLongitude
                  else:
                    tripEndLongitude = previousKnownLongitude
                   
                  tripEndCause = 'SAFETY MESSAGE'
            
            
            if (isVehicleInTrip == 1):
               # Rule #2
               
               # Check if the current position is an end trip
               isVehicleInTrip = checkTripEndSecondRule(currentCanActivity, currentPowerSource, tripStartCanActivity, tripStartPowerSource)
               
               if (isVehicleInTrip == 0):
                  tripDateEnd = currentTimeGMT0
                  
                  if(currentLatitude != 0):
                    tripEndLatitude = currentLatitude
                  else:
                    tripEndLatitude = previousKnownLatitude
                    
                  if(currentLongitude != 0):
                    tripEndLongitude = currentLongitude
                  else:
                    tripEndLongitude = previousKnownLongitude
                    
                  tripEndCause = 'EBS(CAN_ACTIVITY)/POWERSOURCE'
        
        
        try:
          nextPosition = groupValues[i+1]
          nextPositionDate = nextPosition[0]
          nextPositionOdometro = nextPosition[26]
          
        except:
          nextPositionDate = ''
          nextPositionOdometro = 0
          
          isLastVehiclePosition = 1
        
        
        #kmRodado = getKmRodados(currentVehicleOdometro, nextPositionOdometro)
        kmRodado = getKmRodados(currentVehicleOdometro, nextPositionOdometro) + diffLastInfoKmRodados
        
        # Generate trip row
        rowItems['ID_VEICULO'] = int(vehicleId)
        rowItems['DATA_DIRECAO'] = str(currentDataDirecao)
        rowItems['POSDATAHORA'] = str(currentDataHora)
        rowItems['SOFTWARE_VERSION'] = str(currentSoftwareVersion) if currentSoftwareVersion is not None else str(0)
        rowItems['POSIGNICAO'] = int(currentIgnicao)
        rowItems['POSGPS_VELOCIDADE'] = int(currentVelocidade)
        rowItems['LONGITUDE'] = str(currentLongitude)
        rowItems['LATITUDE'] = str(currentLatitude)
        rowItems['KM_RODADO_DENTRO_KM'] = kmRodado
        #rowItems['TEMPO_MINUTO'] = tempoMinutoPosicao
        rowItems['FINALDIA'] = str(currentFinalDia)
        rowItems['INICIODIA'] = str(currentInicioDia)
        rowItems['EPISODIO'] = idEpisodio
        
        if(previousPositionDate == ''):
           rowItems['POSDATAHORA_ANT'] = str(currentInicioDia)
           
        else:
           rowItems['POSDATAHORA_ANT'] = str(previousPositionDate)
        
        if(nextPositionDate == ''):
           rowItems['POSDATAHORA_POS'] = str(currentFinalDia)
           
        else:
           rowItems['POSDATAHORA_POS'] = str(nextPositionDate)
        
        rowItems['POSGPS_VALIDO'] = int(currentGpsValido)
        
        rowItems['STOPMOVE'] = vehicleTrip
        rowItems['BLOQUEIO'] = int(currentBloqueio)
        rowItems['BATERIA'] = currentBateria
        rowItems['MEMORIA'] = int(currentMemoria)
        rowItems['POSID_IBUTTON'] = int(currentIButon)
        rowItems['POSHORIMETRO'] = int(currentHorimetro)
        rowItems['ID_MOTORISTA'] = int(currentIdMotorista)
        rowItems['NOME_MOTORISTA'] = currentNomeMotorista
        rowItems['ID_EVENTO'] = currentIdEvento
        rowItems['BATERIA_INTERNA'] = int(currentBateriaInterna)
        rowItems['LANTERNA'] = int(currentLanterna)
        rowItems['TEMPO_EVENTO'] = currentTempoEvento
        rowItems['ODOMETRO'] = currentVehicleOdometro
        
        rowItems['PESO'] = int(currentPeso)
        rowItems['TRUCKID'] = currentTruckId
        
        rowItems['TRUCK_ASSETS_ARRAY'] = str(currentTruckAssetsIds)
        
        rowItems['LAST_POSITION'] = isLastVehiclePosition
        
        if (isVehicleInTrip == 1):
            rowItems['TRIP_POWER_SOURCE'] = int(tripStartPowerSource)
        else:
            rowItems['TRIP_POWER_SOURCE'] = int(currentPowerSource)
        
        rowItems['TRIP_MODE'] = tripStartTransportMode
        rowItems['DT_BEGIN_TRIP'] = str(tripDateBegin)
        rowItems['DT_END_TRIP'] = str(tripDateEnd)
        rowItems['BEGIN_TRIP_LATITUDE'] = str(tripBeginLatitude)
        rowItems['BEGIN_TRIP_LONGITUDE'] = str(tripBeginLongitude)
        rowItems['END_TRIP_LATITUDE'] = str(tripEndLatitude)
        rowItems['END_TRIP_LONGITUDE'] = str(tripEndLongitude)
        rowItems['TRIP_END_CAUSE'] = str(tripEndCause)
        rowItems['GNSS_DISTANCE'] = currentGnssDistance
        
        rowList.append(rowItems)
        
        if (isVehicleInTrip == 0): # Vehicle has ended a trip
           # Check if the vehicle was on trip and now will begin a stop episode
           if(vehicleTrip == 1):
              idEpisodio = idEpisodio + 1
               
           isVehicleInTrip = 0
           tripStartPowerSource = 0
           tripStartCanActivity = 0
           tripStartTransportMode = 0
           tripMotion = 0
           kmRodado = 0
           vehicleTrip = 0
           tripDateBegin = ''
           tripDateEnd = ''
           tripBeginLatitude = ''
           tripBeginLongitude = ''
           tripEndLatitude = ''
           tripEndLongitude = ''
           
           previousKnownLatitude = ''
           previousKnownLongitude = ''
           
           isLastVehiclePosition = 0
           
           tripEndCause = ''
        
        previousGnssDistance = currentGnssDistance
        previousPositionDate = currentTimeGMT0
        
        if(currentLatitude != 0):
          previousKnownLatitude = currentLatitude
        
        if(currentLongitude != 0):
          previousKnownLongitude = currentLongitude
          
    return rowList
    
if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Define the functions to be used in the dataframe ###
    getTripTimeUdf = udf(getTripTime, FloatType())
    getTripSpeedAverageUdf = udf(getTripSpeedAverage, FloatType())
    getPositionTempoMinutouDF = udf(getPositionTempoMinuto, FloatType())
    getTripDistanceUdf = udf(getTripDistance, FloatType())
    getTruckIdUdf = udf(getTruckId, LongType())
    getTruckAssetsIdsUdf = udf(getTruckAssetsIds, StringType())
    
    # Get current date in GMT-0 from the system to execute the process
    dateToProcess = datetime.now() - timedelta(days = 1)
    
    # Configure the dates for the filter to be used in the query
    fromDateGmt0 = dateToProcess.strftime("%Y-%m-%d") + ' 00:00:00'
    toDateGmt0 = dateToProcess.strftime("%Y-%m-%d") + ' 23:59:59'
    
    inicioDia = dateToProcess.strftime("%Y-%m-%d") + ' 00:00:00'
    fimDia = dateToProcess.strftime("%Y-%m-%d") + ' 23:59:59'
    
    # Get position date short to filter the query
    fromDate = datetime.now() - timedelta(days = 2)
    toDate = datetime.now()
    
    fromDateShort = fromDate.strftime("%Y-%m-%d")
    toDateShort = toDate.strftime("%Y-%m-%d")
    
    ### Query to get necessary info from position table ###
    hiveSql = """SELECT VEICULO AS ID_VEICULO, 
                        DATA_POSICAO_GMT0, 
                        cast(from_unixtime(unix_timestamp(DATA_POSICAO_GMT0), 'yyyy-MM-dd') as timestamp) AS DATA_DIRECAO,
                        DATA_POSICAO_GMT0 AS POSDATAHORA, 
                        COALESCE(SOFTWARE_VERSION, 0) as SOFTWARE_VERSION,
                        IGNICAO AS POSIGNICAO, 
                        VELOCIDADE AS POSGPS_VELOCIDADE, 
                        LATITUDE, 
                        LONGITUDE, 
                        GPS_VALIDO AS POSGPS_VALIDO, 
                        BLOCKVEHICLE AS BLOQUEIO, 
                        '' AS BATERIA, 
                        POS_MEMORIA AS MEMORIA, 
                        0 AS POSID_IBUTTON, 
                        HORIMETRO AS POSHORIMETRO, 
                        ID_MOTORISTA AS ID_MOTORISTA, 
                        '' AS NOME_MOTORISTA, 
                        '' AS ID_EVENTO, 
                        BATERIAINT AS BATERIA_INTERNA, 
                        CASE 
                          WHEN PAYLOAD_DIAG.CONTENT.POWER_SOURCE = TRUE THEN 1 
                          ELSE 0 
                        END AS LANTERNA, 
                        '' AS TEMPO_EVENTO,
                        PAYLOAD_DIAG.CONTENT.VEHICLE_MOTION AS MOTION, 
                        PAYLOAD_DIAG.HEADER.PAYLOAD_CAUSE AS PAYLOAD_CAUSE, 
                        CASE 
                          WHEN PAYLOAD_DIAG.CONTENT.POWER_SOURCE = TRUE THEN 1 
                          ELSE 0 
                        END AS POWER_SOURCE,
                        PAYLOAD_DIAG.CONTENT.GNSS_DISTANCE AS DISTANCE_GNSS, 
                        PAYLOAD_DIAG.CONTENT.CAN_ACTIVITY, 
                        PAYLOAD_DIAG.CONTENT.BATTERY_CHARGE_LEVEL, 
                        PAYLOAD_DIAG.CONTENT.GNSS_ALTITUDE AS ALTITUDE, 
                        COALESCE(PAYLOAD_EBS[0].CONTENT.AXLE_LOAD_SUM_MEAN, 0) AS PESO, 
                        PAYLOAD_WSN[0].CONTENT.VID AS TRUCKID_ARRAY,
                        PAYLOAD_DIAG.CONTENT.BATTERY_VOLTAGE AS DIAG_BATTERY_VOLTAGE,
                        COALESCE(PAYLOAD_EBS[0].CONTENT.AXLE_LOAD_SUM_MEAN, 0) AS AXLE_LOAD_SUM_MEAN,
                        COALESCE(PAYLOAD_EBS[0].CONTENT.ABS_ACTIVE_TOWING_COUNTER, 0) AS ABS_ACTIVE_TOWING_COUNTER,
                        COALESCE(PAYLOAD_EBS[0].CONTENT.YC_SYSTEM_COUNTER, 0) AS YC_SYSTEM_COUNTER,
                        COALESCE(PAYLOAD_EBS[0].CONTENT.ROP_SYSTEM_COUNTER, 0) AS ROP_SYSTEM_COUNTER,
                        COALESCE(PAYLOAD_EBS[0].CONTENT.VDC_ACTIVE_TOWING_COUNTER, 0) AS VDC_ACTIVE_TOWING_COUNTER,
                        PAYLOAD_EBS[0].CONTENT.AXLE_LOAD_SUM_MAX AS EBS_AXLE_LOAD_SUM_MAX,
                        PAYLOAD_EBS[0].CONTENT.AXLE_LOAD_SUM_MEAN AS EBS_AXLE_LOAD_SUM_MEAN,
                        PAYLOAD_EBS[0].CONTENT.AXLE_LOAD_SUM_MIN AS EBS_AXLE_LOAD_SUM_MIN,
                        PAYLOAD_EBS[0].CONTENT.SERVICE_BRAKE_COUNTER AS SERVICE_BRAKE_COUNTER,
                        PAYLOAD_EBS[0].CONTENT.AMBER_WARNING_SIGNAL_COUNTER AS AMBER_WARNING_SIGNAL_COUNTER,
                        PAYLOAD_EBS[0].CONTENT.RED_WARNING_SIGNAL_COUNTER AS RED_WARNING_SIGNAL_COUNTER,
                        PAYLOAD_TPM,
                        PAYLOAD_WSN
                 FROM POSICOES_CRCU 
                 WHERE  DATA_POSICAO_SHORT >= '{0}' 
                  AND   DATA_POSICAO_SHORT <= '{1}'
                  AND   DATA_POSICAO_GMT0 >= '{2}'
                  AND   DATA_POSICAO_GMT0 <= '{3}'""".format(fromDateShort, toDateShort, fromDateGmt0, toDateGmt0)
    
    ### Execute query ###
    vehicles = sqlContext.sql(hiveSql)
    
    ### Add new columns to indicate begin and end day ###
    vehiclesInicioFimDia = vehicles.withColumn('INICIODIA', lit(inicioDia)).withColumn('FINALDIA', lit(fimDia))
    
    ### Redistribute the dataframe according to the column desire into 'n' partitions ###
    vehiclesPartitioned = vehiclesInicioFimDia.repartition(5, "ID_VEICULO")
    
    ### Get only the trip algorithm and stop move fields ###
    vehiclesCastDate = vehiclesPartitioned.select("ID_VEICULO", col("DATA_POSICAO_GMT0").cast("timestamp").alias("DATA_POSICAO_GMT0"), \
          "DATA_DIRECAO", "POSDATAHORA", "POSIGNICAO", "POSGPS_VELOCIDADE", "LATITUDE", "LONGITUDE", "FINALDIA", "INICIODIA", "POSGPS_VALIDO", \
          "BLOQUEIO", "BATERIA", "MEMORIA", "POSID_IBUTTON", "POSHORIMETRO", "ID_MOTORISTA", "NOME_MOTORISTA", "ID_EVENTO", "BATERIA_INTERNA", \
          "LANTERNA", "TEMPO_EVENTO", "MOTION", "PAYLOAD_CAUSE", "POWER_SOURCE", "DISTANCE_GNSS", "CAN_ACTIVITY", "PESO", "TRUCKID_ARRAY", \
          "SOFTWARE_VERSION")
    
    ### Get only the status report field for Electrum ###
    statusDataFrame = vehiclesPartitioned.select(col("ID_VEICULO").alias("ID_VEICULO_STATUS"), col("DATA_POSICAO_GMT0").cast("timestamp").alias("DATE_POSITION_STATUS"), \
          "DIAG_BATTERY_VOLTAGE", "AXLE_LOAD_SUM_MEAN", "ABS_ACTIVE_TOWING_COUNTER", "YC_SYSTEM_COUNTER", "ROP_SYSTEM_COUNTER", \
          "VDC_ACTIVE_TOWING_COUNTER", "EBS_AXLE_LOAD_SUM_MAX", "EBS_AXLE_LOAD_SUM_MEAN", "EBS_AXLE_LOAD_SUM_MIN", "SERVICE_BRAKE_COUNTER", \
          "AMBER_WARNING_SIGNAL_COUNTER", "RED_WARNING_SIGNAL_COUNTER", "CAN_ACTIVITY", "POWER_SOURCE", "PAYLOAD_TPM", "PAYLOAD_WSN", \
          "BATTERY_CHARGE_LEVEL", "ALTITUDE")
    
    # Query to get the correct odometer of the vehicles for the current positions from the stage that was already adjusted in a previous process
    hiveSql = "SELECT ID_VEICULO AS ID_VEICULO_ODOMETRO, \
                      DATA_POSICAO_GMT0 AS DATA_POSICAO_GMT0_ODOMETRO, \
                      ODOMETRO AS POSGPS_ODOMETRO\
               FROM   STAGEODOMETRO"
    
    ### Execute query ###
    veiculosOdometro = sqlContext.sql(hiveSql)
       
    # Join the trip positions with the odometer positions
    joinVeiculosOdometro = vehiclesCastDate.join(veiculosOdometro, (vehiclesCastDate.ID_VEICULO == veiculosOdometro.ID_VEICULO_ODOMETRO) & (vehiclesCastDate.POSDATAHORA == veiculosOdometro.DATA_POSICAO_GMT0_ODOMETRO))
    
    ### Query to get the correct aixs and position of the tire
    sql = "(SELECT distinct RFSID_VEICULO as veiculoobgfinan \
            FROM manutencao.cliente_obg_financeira \
            WHERE RFSTAG_FUNCIONALIDADE = 'SAS_WEB_OPC_FTRUCK') obrigacao"
    
    veiculosObrigacao = sqlContext.read.format("jdbc"). \
		 option("url", dbconf.DOMINIO). \
		 option("driver", "org.postgresql.Driver"). \
		 option("dbtable", sql). \
		 load()
    
    veiculosOf = joinVeiculosOdometro.join(veiculosObrigacao, joinVeiculosOdometro.ID_VEICULO == veiculosObrigacao.veiculoobgfinan, "left_outer")
    
    vehiclesTruck = veiculosOf.withColumn('TRUCKID', getTruckIdUdf('veiculoobgfinan', 'TRUCKID_ARRAY'))
    
    vehiclesTruckAssets = vehiclesTruck.withColumn('TRUCK_ASSETS_ARRAY', getTruckAssetsIdsUdf('TRUCKID_ARRAY'))
    
    # Query to get the correct odometer of the vehicles for the current positions from the stage that was already adjusted in a previous process
    hiveSql = "SELECT ID_VEICULO AS ID_VEICULO_LAST_INFO, \
                      DATA_POSICAO_GMT0 AS DATA_POSICAO_GMT0_LAST_INFO, \
                      ODOMETRO AS ODOMETRO_LAST_INFO, \
                      STOPMOVE AS STOPMOVE_LAST_INFO \
               FROM   STG_LAST_POSITION_INFO"
    
    ### Execute query ###
    lastInfoVehicles = sqlContext.sql(hiveSql)
    
    vehiclesLastInfoTruck = vehiclesTruckAssets.join(lastInfoVehicles,  vehiclesTruckAssets.ID_VEICULO == lastInfoVehicles.ID_VEICULO_LAST_INFO, "left_outer")
    
    ### Transform the current dataframe into a RDD ###
    vehiclesRdd = vehiclesLastInfoTruck.rdd
    
    ### Map the current RDD into key/value pairs ###
    vehiclesRddMapped = vehiclesRdd.map(lambda x: (x['ID_VEICULO'], (x['DATA_POSICAO_GMT0'], x['DATA_DIRECAO'], x['POSDATAHORA'], x['POSIGNICAO'], \
                                                   x['POSGPS_VELOCIDADE'], x['LATITUDE'], x['LONGITUDE'], x['FINALDIA'], x['INICIODIA'], \
                                                   x['POSGPS_VALIDO'], x['BLOQUEIO'], x['BATERIA'], x['MEMORIA'], x['POSID_IBUTTON'], \
                                                   x['POSHORIMETRO'], x['ID_MOTORISTA'], x['NOME_MOTORISTA'], x['ID_EVENTO'], x['BATERIA_INTERNA'], \
                                                   x['LANTERNA'], x['TEMPO_EVENTO'], x['MOTION'], x['PAYLOAD_CAUSE'], x['POWER_SOURCE'], \
                                                   x['DISTANCE_GNSS'], x['CAN_ACTIVITY'], x['POSGPS_ODOMETRO'], x['PESO'], \
                                                   x['TRUCKID'], x['DATA_POSICAO_GMT0_LAST_INFO'], x['ODOMETRO_LAST_INFO'], x['STOPMOVE_LAST_INFO'], \
                                                   x['TRUCK_ASSETS_ARRAY'], x['SOFTWARE_VERSION'])))
    
    # Sort the RDD values according to the 'POSITION_DATE'
    vehiclesRddGrouped = vehiclesRddMapped.groupByKey().mapValues(lambda vs: sorted(vs, key=lambda x: x[0:1]))
    
    # Proccess the trips
    processedRdd = vehiclesRddGrouped.flatMap(processVehicleTrip)
    
    # Check if any trip was created
    if(processedRdd.isEmpty() == False):
       # Transform the resulted RDD into a Spark Dataframe
       sparkDataframe = processedRdd.toDF()
       
       # Adjust the BEGIN and END date and BEGIN and END latitude/longitude columns of the trips
       tripVehicleEndDate = sparkDataframe.select(col("ID_VEICULO").alias("AUX_VEHICLE"), col("DT_BEGIN_TRIP").alias("AUX_DT_BEGIN_TRIP"), \
                                                  col("DT_END_TRIP").alias("AUX_DT_END_TRIP"), \
                                                  col("END_TRIP_LATITUDE").alias("AUX_END_TRIP_LATITUDE"), \
                                                  col("END_TRIP_LONGITUDE").alias("AUX_END_TRIP_LONGITUDE"), \
                                                  col("TRIP_END_CAUSE").alias("AUX_TRIP_END_CAUSE"))\
                                          .filter("length(DT_END_TRIP) > 0")\
                                          .distinct()
       
       joinTripDataFrame = sparkDataframe.join(tripVehicleEndDate, (sparkDataframe.ID_VEICULO == tripVehicleEndDate.AUX_VEHICLE) & (sparkDataframe.DT_BEGIN_TRIP == tripVehicleEndDate.AUX_DT_BEGIN_TRIP), "left_outer")
       
       ### Store the last info position - BEGIN ###
       lastVehiclesPosition = joinTripDataFrame.filter("LAST_POSITION = 1").select("ID_VEICULO", col("POSDATAHORA").cast("timestamp").alias("DATA_POSICAO_GMT0"), \
                                                       col("STOPMOVE").cast("integer").alias("STOPMOVE"), "ODOMETRO")
       
       lastVehiclesPosition.write.mode("overwrite").saveAsTable("stg_last_position_info")
       ### Store the last info position - END ###
       
       ### STG EPISODIO STOP MOVE - BEGIN ###
       # Adjust the columns alias and type for the EPISODIO STOP MOVE table
       stopMoveDataFrame = joinTripDataFrame.select("ID_VEICULO", col("DATA_DIRECAO").cast("timestamp").alias("DATA_DIRECAO"), \
               col("POSDATAHORA").cast("timestamp").alias("POSDATAHORA"), col("POSIGNICAO").cast("integer").alias("POSIGNICAO"), \
               col("POSGPS_VELOCIDADE").cast("integer").alias("POSGPS_VELOCIDADE"), "LATITUDE", "LONGITUDE", \
               col("KM_RODADO_DENTRO_KM").cast("float").alias("KM_RODADO_DENTRO_KM"), col("ODOMETRO").cast("integer").alias("POSGPS_ODOMETRO"), \
               col("FINALDIA").cast("timestamp").alias("FINALDIA"), col("INICIODIA").cast("timestamp").alias("INICIODIA"), \
               col("POSDATAHORA_ANT").cast("timestamp").alias("POSDATAHORA_ANT"), \
               col("POSDATAHORA_POS").cast("timestamp").alias("POSDATAHORA_POS"), col("POSGPS_VALIDO").cast("integer").alias("POSGPS_VALIDO"), \
               col("STOPMOVE").cast("integer").alias("STOPMOVE"), col("EPISODIO").cast("integer").alias("EPISODIO"), \
               col("BLOQUEIO").cast("integer").alias("BLOQUEIO"), col("BATERIA").cast("integer").alias("BATERIA"), \
               col("MEMORIA").cast("integer").alias("MEMORIA"), col("POSID_IBUTTON").cast("integer").alias("POSID_IBUTTON"), \
               col("POSHORIMETRO").cast("integer").alias("POSHORIMETRO"), col("ID_MOTORISTA").cast("integer").alias("ID_MOTORISTA"), \
               "NOME_MOTORISTA", col("ID_EVENTO").cast("integer").alias("ID_EVENTO"), col("BATERIA_INTERNA").cast("integer").alias("BATERIA_INTERNA"), \
               col("LANTERNA").cast("integer").alias("LANTERNA"), col("TEMPO_EVENTO").cast("integer").alias("TEMPO_EVENTO"), \
               col("PESO").cast("integer").alias("PESO"), col("TRUCKID").cast("long").alias("TRUCKID"), \
               col("TRUCK_ASSETS_ARRAY").cast("string").alias("TRUCK_ASSETS_ARRAY")).distinct()
       
       stopMoveTempoMinuto = stopMoveDataFrame.withColumn('TEMPO_MINUTO', getPositionTempoMinutouDF('POSDATAHORA', 'POSDATAHORA_POS', 'POSDATAHORA_ANT', 'INICIODIA'))
       
       # Remove the duplicated rows #
       stopMoveAgrouped = stopMoveTempoMinuto.groupBy("ID_VEICULO", "POSDATAHORA").agg(max("DATA_DIRECAO").alias("DATA_DIRECAO"), \
                           max("POSIGNICAO").alias("POSIGNICAO"), max("POSGPS_VELOCIDADE").alias("POSGPS_VELOCIDADE"), max("LATITUDE").alias("LATITUDE"), \
                           max("LONGITUDE").alias("LONGITUDE"), max("KM_RODADO_DENTRO_KM").alias("KM_RODADO_DENTRO_KM"), max("POSGPS_ODOMETRO").alias("POSGPS_ODOMETRO"), \
                           max("FINALDIA").alias("FINALDIA"), max("INICIODIA").alias("INICIODIA"), min("POSDATAHORA_ANT").alias("POSDATAHORA_ANT"), \
                           max("POSDATAHORA_POS").alias("POSDATAHORA_POS"), max("POSGPS_VALIDO").alias("POSGPS_VALIDO"), max("STOPMOVE").alias("STOPMOVE"), \
                           max("EPISODIO").alias("EPISODIO"), max("BLOQUEIO").alias("BLOQUEIO"), max("BATERIA").alias("BATERIA"), max("MEMORIA").alias("MEMORIA"), \
                           max("POSID_IBUTTON").alias("POSID_IBUTTON"), max("POSHORIMETRO").alias("POSHORIMETRO"), max("ID_MOTORISTA").alias("ID_MOTORISTA"), \
                           max("NOME_MOTORISTA").alias("NOME_MOTORISTA"), max("ID_EVENTO").alias("ID_EVENTO"), max("BATERIA_INTERNA").alias("BATERIA_INTERNA"), \
                           max("LANTERNA").alias("LANTERNA"), max("TEMPO_EVENTO").alias("TEMPO_EVENTO"), max("TEMPO_MINUTO").alias("TEMPO_MINUTO"), \
                           when(max("PESO") == 0, None).otherwise(max("PESO")).alias("PESO"), max("TRUCKID").alias("TRUCKID"), \
                           max("TRUCK_ASSETS_ARRAY").alias("TRUCK_ASSETS_ARRAY"))
       
       stopMoveFinal = stopMoveAgrouped.withColumn('VEICULO_GMT', lit('')) \
                                       .withColumn('DATA_INSERT', lit(''))
       
       ## Persist stage table in hive to be consumed by a Pentaho process to load the data into DW (Oracle)
       stopMoveFinal.write.mode("overwrite").saveAsTable("stg_episodio_stop_move")
       ### STG EPISODIO STOP MOVE - END ###
       
       ### FT TRIP REPORT DETAILED  (OPERATIONAL PORTAL) - BEGIN ###
       # Put togueter the trip results with the status data frame
       fullStatusReportDataFrame = joinTripDataFrame.join(statusDataFrame, (joinTripDataFrame.ID_VEICULO == statusDataFrame.ID_VEICULO_STATUS) & (joinTripDataFrame.POSDATAHORA == statusDataFrame.DATE_POSITION_STATUS))
       
       # Filter only the trip data
       detailedDataFrame = fullStatusReportDataFrame.filter("STOPMOVE = 1").select(col("ID_VEICULO").alias("VEHICLE"), \
               col("POSDATAHORA").cast("timestamp").alias("DATE_POSITION"), col("DT_BEGIN_TRIP").cast("timestamp").alias("DT_BEGIN_TRIP"), \
               col("AUX_DT_END_TRIP").cast("timestamp").alias("DT_END_TRIP"), col("CAN_ACTIVITY").cast("integer").alias("CAN_ACTIVITY"), \
               col("POWER_SOURCE").cast("integer").alias("POWER_SOURCE"), col("DIAG_BATTERY_VOLTAGE").cast("integer").alias("DIAG_BATTERY_VOLTAGE"), \
               col("AXLE_LOAD_SUM_MEAN").cast("integer").alias("AXLE_LOAD_SUM_MEAN"), col("ABS_ACTIVE_TOWING_COUNTER").cast("integer").alias("ABS_ACTIVE_TOWING_COUNTER"), \
               col("YC_SYSTEM_COUNTER").cast("integer").alias("YC_SYSTEM_COUNTER"), col("ROP_SYSTEM_COUNTER").cast("integer").alias("ROP_SYSTEM_COUNTER"), \
               col("VDC_ACTIVE_TOWING_COUNTER").cast("integer").alias("VDC_ACTIVE_TOWING_COUNTER"), \
               col("EBS_AXLE_LOAD_SUM_MAX").cast("integer").alias("EBS_AXLE_LOAD_SUM_MAX"), col("EBS_AXLE_LOAD_SUM_MEAN").cast("integer").alias("EBS_AXLE_LOAD_SUM_MEAN"), \
               col("EBS_AXLE_LOAD_SUM_MIN").cast("integer").alias("EBS_AXLE_LOAD_SUM_MIN"), col("SERVICE_BRAKE_COUNTER").cast("integer").alias("SERVICE_BRAKE_COUNTER"), \
               col("AMBER_WARNING_SIGNAL_COUNTER").cast("integer").alias("AMBER_WARNING_SIGNAL_COUNTER"), col("RED_WARNING_SIGNAL_COUNTER").cast("integer").alias("RED_WARNING_SIGNAL_COUNTER"), \
               col("ALTITUDE").cast("float").alias("ALTITUDE"), col("BATTERY_CHARGE_LEVEL").cast("integer").alias("BATTERY_CHARGE_LEVEL"), "PAYLOAD_TPM", "PAYLOAD_WSN")
       
       # Create a temporary table of the dataframe
       detailedDataFrame.createOrReplaceTempView("detailedDataFrameTable")
       
       detailedExploded = sqlContext.sql("""SELECT VEHICLE,
                                                   DT_BEGIN_TRIP,
                                                   DT_END_TRIP,
                                                   DATE_POSITION,
                                                   DIAG_BATTERY_VOLTAGE,
                                                   CAN_ACTIVITY, 
                                                   POWER_SOURCE,
                                                   AXLE_LOAD_SUM_MEAN,
                                                   ABS_ACTIVE_TOWING_COUNTER,
                                                   YC_SYSTEM_COUNTER,
                                                   ROP_SYSTEM_COUNTER,
                                                   VDC_ACTIVE_TOWING_COUNTER,
                                                   EBS_AXLE_LOAD_SUM_MAX,
                                                   EBS_AXLE_LOAD_SUM_MEAN,
                                                   EBS_AXLE_LOAD_SUM_MIN,
                                                   SERVICE_BRAKE_COUNTER,
                                                   AMBER_WARNING_SIGNAL_COUNTER,
                                                   RED_WARNING_SIGNAL_COUNTER,
                                                   A.TPM.CONTENT.DEVICE_SERIAL AS DEVICE_SERIAL,
                                                   A.TPM.CONTENT.DEVICE_POSITION AS DEVICE_POSITION,
                                                   A.TPM.CONTENT.HIGH_TEMPERATURE_THR AS HIGH_TEMPERATURE_THR,
                                                   A.TPM.CONTENT.LOW_PRESSURE_ALERT_THR AS LOW_PRESSURE_ALERT_THR,
                                                   A.TPM.CONTENT.LOW_PRESSURE_WARNING_THR AS LOW_PRESSURE_WARNING_THR,
                                                   A.TPM.CONTENT.NOMINAL_PRESSURE AS NOMINAL_PRESSURE,
                                                   B.SENSOR.IDENTIFIER AS SENSOR_IDENTIFIER,
                                                   B.SENSOR.INDEX AS SENSOR_INDEX,
                                                   B.SENSOR.PRESSURE AS SENSOR_PRESSURE,
                                                   B.SENSOR.TEMPERATURE AS SENSOR_TEMPERATURE,
                                                   B.SENSOR.ALERT_BATTERY AS SENSOR_ALERT_BATTERY,
                                                   B.SENSOR.COM AS SENSOR_COM,
                                                   B.SENSOR.`CONF` AS SENSOR_CONF,
                                                   B.SENSOR.STATUS AS SENSOR_STATUS,
                                                   D.WSN_TES.TEMPERATURE AS WSN_TEMPERATURE,
                                                   D.WSN_TES.INDEX AS WSN_INDEX,
                                                   D.WSN_TES.BATTERY_STATUS AS WSN_TES_BATTERY_STATUS,
                                                   E.WSN_VID.INDEX AS WSNVIDPOSITION,
                                                   E.WSN_VID.ID AS WSNVIDID,
                                                   E.WSN_VID.TAG AS WSNVIDTAG,
                                                   E.WSN_VID.BATTERY_STATUS AS WSN_VID_BATTERY_STATUS,
                                                   ALTITUDE,
                                                   BATTERY_CHARGE_LEVEL
                                            FROM detailedDataFrameTable
                                            LATERAL VIEW OUTER EXPLODE(PAYLOAD_TPM) A AS TPM
                                            LATERAL VIEW OUTER EXPLODE(A.TPM.CONTENT.SENSORS) B AS SENSOR
                                            LATERAL VIEW OUTER EXPLODE(PAYLOAD_WSN) C AS WSN
                                            LATERAL VIEW OUTER EXPLODE(C.WSN.CONTENT.TES) D AS WSN_TES
                                            LATERAL VIEW OUTER EXPLODE(C.WSN.CONTENT.VID) E AS WSN_VID""")
       
       s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
       redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
       
       detailedExploded.write \
           .format("com.databricks.spark.redshift") \
           .option("url", redshiftURL) \
           .option("tempdir", s3TempDir) \
           .option("dbtable", "FT_TRIP_REPORT_DETAILED") \
           .mode("append") \
           .save()
       
       ### FT TRIP REPORT DETAILED  (OPERATIONAL PORTAL) - END ###
       
       ### FT TRIP REPORT AGG (OPERATIONAL PORTAL) - BEGIN ###
       # Adjust the columns alias and type for the FT_TRIP_REPORT_AGG table
       tripReportDataframe = joinTripDataFrame.filter("STOPMOVE = 1").select(col("ID_VEICULO").alias("VEHICLE"), col("DT_BEGIN_TRIP").cast("timestamp").alias("DT_BEGIN_TRIP"), \
               col("AUX_DT_END_TRIP").cast("timestamp").alias("DT_END_TRIP"), "TRIP_MODE", \
               col("BEGIN_TRIP_LATITUDE").cast("float").alias("BEGIN_TRIP_LATITUDE"), col("BEGIN_TRIP_LONGITUDE").cast("float").alias("BEGIN_TRIP_LONGITUDE"), \
               col("AUX_END_TRIP_LATITUDE").cast("float").alias("END_TRIP_LATITUDE"), col("AUX_END_TRIP_LONGITUDE").cast("float").alias("END_TRIP_LONGITUDE"), \
               col("AUX_TRIP_END_CAUSE").alias("TRIP_END_CAUSE"), col("ODOMETRO").cast("integer").alias("POSGPS_ODOMETRO"), "EPISODIO", "INICIODIA", \
               col("GNSS_DISTANCE").cast("integer").alias("GNSS_DISTANCE"), col("SOFTWARE_VERSION").cast("string").alias("SOFTWARE_VERSION"))
       
       # Get the distance in km of each trip
       tripReporteAgg = tripReportDataframe.groupBy("VEHICLE", "DT_BEGIN_TRIP", "DT_END_TRIP", "TRIP_MODE", "BEGIN_TRIP_LATITUDE", \
                                                    "EPISODIO", "INICIODIA", "BEGIN_TRIP_LONGITUDE", "END_TRIP_LATITUDE", "END_TRIP_LONGITUDE", \
                                                    "TRIP_END_CAUSE")\
                                           .agg(min("GNSS_DISTANCE").alias("BEGIN_GNSS_DISTANCE"), \
                                                max("GNSS_DISTANCE").alias("END_GNSS_DISTANCE"), \
                                                max("SOFTWARE_VERSION").alias("SOFTWARE_VERSION"))
       
       # Get the time in minutes of each trip
       tripReportTripTime = tripReporteAgg.withColumn('TRIP_TIME', getTripTimeUdf('DT_BEGIN_TRIP', 'DT_END_TRIP'))
       
       # Get the distance of the trips
       tripReportDistance = tripReportTripTime.withColumn('DISTANCE_KM', getTripDistanceUdf("BEGIN_GNSS_DISTANCE", "END_GNSS_DISTANCE"))
       
       # Get the speed avarege of each trip
       tripReportTripAvgSpeed = tripReportDistance.withColumn('SPEED_AVG', getTripSpeedAverageUdf('DISTANCE_KM', 'TRIP_TIME'))
       
       # Adjust the trip mode of each trip
       tripReportTripMode = tripReportTripAvgSpeed.withColumn('TRIP_MODE', when(tripReportTripAvgSpeed.TRIP_MODE == 1, 'POWERED').when(tripReportTripAvgSpeed.TRIP_MODE == 2, 'BATTERY').when(tripReportTripAvgSpeed.TRIP_MODE == 3, 'MULTIMODAL').when(tripReportTripAvgSpeed.TRIP_MODE == 4, 'UNDEFINED').otherwise('UNDEFINED'))
       
       tripReportAdjustDateBegin = tripReportTripMode.withColumn('NEW_DT_BEGIN_TRIP', when(tripReportTripMode.EPISODIO == 1, tripReportTripMode.INICIODIA).otherwise(tripReportTripMode.DT_BEGIN_TRIP))
       
       # Get final columns of the report
       tripFinalReportTripMode = tripReportAdjustDateBegin.select("VEHICLE", col("NEW_DT_BEGIN_TRIP").alias("DT_BEGIN_TRIP"), "DT_END_TRIP", "TRIP_MODE", "BEGIN_TRIP_LATITUDE", \
          "BEGIN_TRIP_LONGITUDE", "END_TRIP_LATITUDE", "END_TRIP_LONGITUDE", "TRIP_END_CAUSE", "TRIP_TIME", "SPEED_AVG", \
          "BEGIN_GNSS_DISTANCE", "END_GNSS_DISTANCE", "SOFTWARE_VERSION")
       
       # Load the trip report on table FT_TRIP_REPORT_AGG in Redshift
       tripFinalReportTripMode.write \
           .format("com.databricks.spark.redshift") \
           .option("url", redshiftURL) \
           .option("tempdir", s3TempDir) \
           .option("dbtable", "FT_TRIP_REPORT_AGG") \
           .mode("append") \
           .save()
       ### FT TRIP REPORT AGG (OPERATIONAL PORTAL) - END ###
       
       ### STG FRIDGE STOP MOVE - BEGIN ###
       fridgeReport = fullStatusReportDataFrame.filter("STOPMOVE = 1") \
                                               .select("ID_VEICULO", \
                                                       "POSDATAHORA", \
                                                       col("DATA_DIRECAO").cast("timestamp").alias("DATA_DIRECAO"), \
                                                       col("EPISODIO").cast("integer").alias("EPISODIO"), \
                                                       "PAYLOAD_WSN")
       
       fridgeReport.createOrReplaceTempView("detailedFridgeTable")
       
       detailedFridge = sqlContext.sql("""SELECT ID_VEICULO,
                                                 POSDATAHORA,
                                                 DATA_DIRECAO,
                                                 EPISODIO,
                                                 D.WSN_TES.TEMPERATURE AS WSN_TEMPERATURE,
                                                 D.WSN_TES.INDEX AS WSN_INDEX,
                                                 D.WSN_TES.BATTERY_STATUS AS WSN_TES_BATTERY_STATUS
                                          FROM detailedFridgeTable
                                          LATERAL VIEW OUTER EXPLODE(PAYLOAD_WSN) C AS WSN
                                          LATERAL VIEW OUTER EXPLODE(C.WSN.CONTENT.TES) D AS WSN_TES""")
       
       detailedFridge.distinct().write.mode("overwrite").saveAsTable("stg_fridge_stop_move")
       ### STG FRIDGE STOP MOVE - END ###






