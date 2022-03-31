#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#------------------------------------------------------------------------
# MQTT to TimescaleDB
#
# Copyright 2016-2022 xbgmsharp <xbgmsharp@gmail.com>. All Rights Reserved.
# License:  GNU General Public License version 3 or later; see LICENSE.txt
# Website:  https://trakt.tv, https://github.com/xbgmsharp/trakt
#------------------------------------------------------------------------
#
# Purpose:
# Subscribe to an MQTT topic and insert message into a TimescaleDB
#
# Requirement on Ubuntu/Debian Linux system
# apt-get install python3-dateutil python3-paho-mqtt python3-psycopg2
#
# Requirement on Windows on Python 3
# <python dir>\Scripts\pip3.exe install requests simplejson
#
# Usage: python3 mosquitto_to_timescaledb.py \
#           --msqt_topic "sensor/output --msqt_host "192.168.1.12" --msqt_port 1883 \
#           --ts_host "192.168.1.12" --ts_port 5432 \
#           --ts_username postgres --ts_password postgres1234 --ts_database demo_iot
#

import argparse
import json
import logging
import sys, os
from datetime import datetime
import uuid

try:
        import paho.mqtt.client as mqtt
        import psycopg2
except:
        sys.exit("Please use your favorite method to install the following module paho.mqtt.client and psycopg2 to use this program")

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

args = argparse.Namespace
ts_connection: str = ""
connection: str = ""

desc="""This program subscribe to an MQTT topic and insert message into a TimescaleDB."""

epilog="""Subscribe to an MQTT topic.
Insert message into a TimescaleDB."""

def main():
    global args
    global ts_connection
    global connection

    args = parse_args()

    try:
        database = os.environ['DATABASE']
    except KeyError:
        print('Environment variable %s does not exist' % ('DATABASE'))

    ts_connection = "postgres://{}:{}@{}:{}/{}".format(args.ts_username, args.ts_password, args.ts_host,
                                                       args.ts_port, args.ts_database)
    mqtt_connection = "mqtt://{}:{}@{}:{}/{}".format(args.mqqt_username, args.mqqt_password, args.mqqt_host,
                                                       args.mqqt_port, args.mqqt_topic)
    logger.debug("TimescaleDB connection: {}".format(ts_connection))
    logger.debug("MQTT connection: {}".format(mqtt_connection))

    client_id = f'python-mqtt-{args.ts_database}-{uuid.uuid4()}'
    client = mqtt.Client(client_id)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.mqqt_host, args.mqqt_port, 60)
    #client.username_pw_set(args.mqqt_username, args.mqqt_password)

    # Create table if not exist
    try:
        connection = psycopg2.connect(ts_connection, connect_timeout=3)
        cursor = connection.cursor()
        # SQL query to create a new table
        create_table_query = """-- Step 1: Define regular table
                                CREATE TABLE IF NOT EXISTS sensor_metrics (
                                   time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                                   device_id text NOT NULL,
                                   path text NOT NULL,
                                   value DOUBLE PRECISION NULL
                                );
                                -- Step 2: Turn into hypertable
                                SELECT create_hypertable('sensor_metrics','time');"""
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except psycopg2.DatabaseError as error:
        logger.warning("Exception: {}".format(error.pgerror))
    except psycopg2.OperationalError as error:
        logger.error("Exception: {}".format(error.pgerror))
    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.debug("PostgreSQL TABLE connection is closed")

    # MQTT loop
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    try:
        with psycopg2.connect(ts_connection, connect_timeout=3) as connection:
            client.loop_forever()
    except psycopg2.OperationalError as error:
        logger.error("Exception: {}".format(error.pgerror))
    finally:
        if connection:
            connection.close()
            logger.debug("PostgreSQL data ingest connection is closed")

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.debug("MQTT - Connected with result code {}".format(str(rc)))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(args.mqqt_topic)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.debug("MQTT - Topic: {}, Message Payload: {}".format(msg.topic, str(msg.payload)))
    publish_message_to_db(msg)

def date_converter(o):
    if isinstance(o, datetime):
        return o.__str__()

def publish_message_to_db(message):
    global connection
    message_payload = json.loads(message.payload)
    #logger.debug("message.payload: {}".format(json.dumps(message_payload, default=date_converter)))

    sql = """INSERT INTO sensor_metrics(time, device_id, path, value)
                 VALUES (%s, %s, %s, %s);"""

    data = (
        message_payload["time"],
        message_payload["context"], 
        message_payload["path"],
        message_payload["value"]
    )

    #logger.debug("PostgreSQL - sql.insert: {} {}".format(sql, data))
    try:
        with connection.cursor() as curs:
            try:
                curs.execute(sql, data)
                logger.debug("PostgreSQL - sql.insert: {} {}".format(sql, data))
            except psycopg2.Error as error:
                logger.error("Exception: {}".format(error.pgerror))
            except Exception as error:
                logger.error("Exception: {}".format(error))
    except psycopg2.OperationalError as error:
        logger.error("Exception: {}".format(error.pgerror))
    finally:
        connection.commit()


# Read in command-line parameters
def parse_args():
    parser = argparse.ArgumentParser(description=desc, epilog=epilog)
    parser.add_argument('--mqqt_topic', help='MQTT topic', default='+/signalk/delta')
    parser.add_argument('--mqqt_host', help='MQTT host', default='172.30.0.1')
    parser.add_argument('--mqqt_port', help='MQTT port', type=int, default=1883)
    parser.add_argument('--mqqt_username', help='MQTT username', default='')
    parser.add_argument('--mqqt_password', help='MQTT password', default='')
    parser.add_argument('--ts_host', help='TimescaleDB host', default='172.30.0.1')
    parser.add_argument('--ts_port', help='TimescaleDB port', type=int, default=5432)
    parser.add_argument('--ts_username', help='TimescaleDB username', default='username')
    parser.add_argument('--ts_password', help='TimescaleDB password', default='password')
    parser.add_argument('--ts_database', help='TimescaleDB database', default='example')

    return parser.parse_args()


if __name__ == "__main__":
    main()