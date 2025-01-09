#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Created on 08/06/2020
Ricardo Costardi

"""

import json
import sys, errno
import cx_Oracle
import logging
import pandas as pd

from datetime import datetime
from kafka import KafkaConsumer

from sql_codes import insert_postion_ebpms

KAFKA_AUTO_OFFSET_RESET = "earliest"
KAFKA_ENABLE_AUTO_COMMIT = False
KAFKA_CONSUMER_TIMEOUT_MS = 150000
KAFKA_MAX_POLL_INTERVAL_MS = 100000
KAFKA_SESSION_TIMEOUT_MS = 300000
KAFKA_USE_RD = False

class EbpmsProcess():

    def __init__(self, dbclass=cx_Oracle, kafka_client_class=KafkaConsumer):
        self.sql=None
        self.kafka_client_class = kafka_client_class
        self.dbclass = dbclass

    def set_database_connection(self, login, password, host, database):
        """create a Oracle DB connection

        Args:
            login (str): db login username
            password (str): db user password
            host (str): ip address of db host
            database (str): database name
        """
        self.con = None
        self.con = self.dbclass.connect(
        "{0}/{1}@{2}/{3}".format(login,password,host,database)
        )
        self.cur = self.con.cursor()        
        
    def set_kafka_consumer(self, kafka_hosts, kafka_topic, kafka_consumer_group_id):
        """create a kafka consumer

        Args:
            kafka_hosts (str): kafka hosts
            kafka_topic (str): name of the kafka topic
            kafka_consumer_group_id (str): name of the consumer group
        """
              
        self.ebpms_consumer = self.kafka_client_class(kafka_topic,
                                                     bootstrap_servers=kafka_hosts,
                                                     auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                                                     enable_auto_commit=KAFKA_ENABLE_AUTO_COMMIT,
                                                     group_id=kafka_consumer_group_id,
                                                     session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS)

      
    def execute_merge(self, sql_params) -> bool:
        """Attempts to insert in the FT_PAYLOAD_EBPMS table the validated ebpms positions.

        Args:
            sql_params (str): The string that is the sql command to be executed against the database
        """
        try:
            self.cur.execute(sql_params)
            logging.info("Merge executed!")
            self.con.commit()
            return True
        except cx_Oracle.DatabaseError as e:
            logging.error(str(e))
            raise e


    def extract_payload_ebpms(self, position) -> (str, bool):
        """Checks if the payload, which is still in hexadecimal, is the ebpms payload,
         if it is, it returns the payload.

        Args:
            position (KafkaConsumer.message): The payload still in hexadeciamal
        """

        try:

            if position.get("Payload") [0:4] == '01FD' and position.get('SoftwareVersion') in (['41603828','41314328','41603833','41314333', '41603844']):
                payebpms = bytes.fromhex(position.get('Payload'))
                ASCII = payebpms[15:49]
                data = ASCII.decode(encoding="utf-8")
                data = data.split('/')

                if str(int(data[1].split('-')[0], 16)) != '00000000':
                    ebpms_ok = position
                    return (ebpms_ok)
        except ValueError as e:
            logging.error(f"Error when trying to get the software version, error: {e}")
        return False

    def parser_ebpms(self, position) -> pd.DataFrame:
        """Responsible for making slices and converting from hexadecimal to ascII

        Args:
            position (KafkaConsumer.message): The payload still in hexadeciamal
        """

        try:

            payebpms = bytes.fromhex(position.get('Payload'))
            unitidentifier = bytes.fromhex(position.get('UnitIdentifier'))
            sessionkey = bytes.fromhex(position.get('SessionKey'))
            ASCII = payebpms[15:49]
            data = ASCII.decode(encoding="utf-8")
            data = data.split('/')
            unitidentifier = int.from_bytes(unitidentifier, 'big')
            sessionkey = int.from_bytes(sessionkey, 'big')
            softwareversion = position.get('SoftwareVersion')
            hdr_payload_version = int.from_bytes(payebpms[0:1], 'little')
            hdr_payload_type = int.from_bytes(payebpms[1:2], 'little')
            hdr_payload_cause = int.from_bytes(payebpms[2:3], 'little')
            hdr_generation_number = int.from_bytes(payebpms[3:4], 'little')
            hdr_payload_number = int.from_bytes(payebpms[4:5], 'little')
            hdr_message_date_time = datetime.fromtimestamp(int.from_bytes(payebpms[5:9], 'little'))
            free_ebpms_retarder = 0.1 * int(data[0], 16)
            free_ebpms_time = datetime.fromtimestamp(int(data[1].split('-')[0], 16))
            free_ebpms_duration = 0.001 * int(data[1].split('-')[1], 16)
            free_ebpms_speed_average = int(data[2], 16)
            free_ebpms_altitude_variation = int(data[3], 16)
            free_ebpms_speed_begin = int(data[4].split('-')[0], 16)
            free_ebpms_speed_end = int(data[4].split('-')[1], 16)
            free_ebpms_brake_average = 0.01 * int(data[5], 16)
            speed_variation = (free_ebpms_speed_end - free_ebpms_speed_begin) / 3.6

            data = [unitidentifier, sessionkey, softwareversion, hdr_payload_version,
                    hdr_payload_type, hdr_payload_cause, hdr_generation_number,
                    hdr_payload_number, hdr_message_date_time, free_ebpms_retarder,
                    free_ebpms_time, free_ebpms_duration, free_ebpms_speed_average,
                    free_ebpms_altitude_variation, free_ebpms_speed_begin,
                    free_ebpms_speed_end, free_ebpms_brake_average, speed_variation]

            df_ebpms = pd.DataFrame([data], columns=['unitidentifier','sessionkey','softwareversion','hdr_payload_version',
                                              'hdr_payload_type','hdr_payload_cause','hdr_generation_number',
                                              'hdr_payload_number','hdr_message_date_time','free_ebpms_retarder',
                                              'free_ebpms_time','free_ebpms_duration','free_ebpms_speed_average',
                                              'free_ebpms_altitude_variation','free_ebpms_speed_begin',
                                              'free_ebpms_speed_end','free_ebpms_brake_average','speed_variation'])

            return df_ebpms
        except ValueError as e:
            logging.error("Payload not entered. Error: {1}, UnitIdentifier = {0} ".format(
                                                                                    position.get('UnitIdentifier'), e))
            return pd.DataFrame()


    def process_message(self, message) -> bool:
        """Orchestrate the function calling part to see if it is paylaod ebpms,
         translate the paylaod ebpms and insert it into the database.

        Args:
            message (KafkaConsumer.message): The payload still in hexadeciamal
        """
        position = json.loads(message.value)
        position = self.extract_payload_ebpms(position=position)

        if position != False:
            logging.info("Payload ebpms found!")
            df_ebpms = self.parser_ebpms(position)
            if not df_ebpms.empty:

                sql_params = insert_postion_ebpms.format(str(df_ebpms['unitidentifier'][0]),
                                                                  str(df_ebpms['sessionkey'][0]),
                                                                  str(df_ebpms['softwareversion'][0]),
                                                                  df_ebpms['hdr_payload_version'][0],
                                                                  df_ebpms['hdr_payload_type'][0],
                                                                  df_ebpms['hdr_payload_cause'][0],
                                                                  df_ebpms['hdr_generation_number'][0],
                                                                  df_ebpms['hdr_payload_number'][0],
                                                                  df_ebpms['hdr_message_date_time'][0],
                                                                  df_ebpms['free_ebpms_retarder'][0],
                                                                  df_ebpms['free_ebpms_time'][0],
                                                                  df_ebpms['free_ebpms_duration'][0],
                                                                  df_ebpms['free_ebpms_speed_average'][0],
                                                                  df_ebpms['free_ebpms_altitude_variation'][0],
                                                                  df_ebpms['free_ebpms_speed_begin'][0],
                                                                  df_ebpms['free_ebpms_speed_end'][0],
                                                                  df_ebpms['free_ebpms_brake_average'][0],
                                                                  df_ebpms['speed_variation'][0])

                if self.execute_merge(sql_params):
                    return True
        return False

    def main(self):
        """It consumes the kafka messageria and calls the function to start processing.
        """
        
        try:
            for message in self.ebpms_consumer:
                    logging.info('Read message!')
                    is_ok = self.process_message(message=message)
                    if not is_ok:
                        logging.info(f"Error when trying to process the following message: {message.value}")
                    self.ebpms_consumer.commit()
            return True
        except Exception as e:
            logging.error(e)
            sys.exit(errno.EINTR)
