#!/usr/bin/python

###############################################################################
# This script implements a mqtt client (paho) that subscribes to predefined
# topics (mqtt_topics), converts the payload if needed  and
# stores the payload in the following storage backends:
# MongoDB, ElasticSearch, InfluxDB
#
# Use these scripts at your own risk.
#
# (c) 2015 Swisscom - Andreas Walser
###############################################################################

import paho.mqtt.client as mqtt
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from influxdb import InfluxDBClient
from datetime import datetime
import xml.etree.ElementTree as ET
import json
import logging
import logging.handlers

# Logging handler #############################################################
LOG_FILENAME = '/data/mqtt2storage/log/logging_mqtt2storage.out'
# Set up a specific logger with our desired output level
logger = logging.getLogger('mqtt2storage')
logger.setLevel(logging.INFO)
# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=2*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s;%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Libelium variables #########################################################
libelium_customer = 'octopus'

# mongoDB variables ###########################################################
#mongodb_uri = 'mongodb://localhost:27017'
mongodb_uri = 'mongodb://mongo-dev:27017'
#mongodb_uri = 'mongodb://username:password@localhost:port/db'

# mqtt variables ##############################################################
# mqtt topics should have the following format: swisscom/<customer>/<device>
# payload should have the following format {deviceID, sensor, value, date}
# adding '/#' allows to subscribe to all sub-topics
# since Libelium published to hard coded topics we need a work-around and
# subscribe to two topics
mqtt_topic_libelium = 'Libelium/#'
mqtt_topic_swisscom = 'swisscom/#'
#mqtt_host = 'localhost'
mqtt_host = 'mqtt-dev'
mqtt_port = 1883
#mqtt_username = ''
#mqtt_password = ''

# ElasticSearch variables #####################################################
#elasticsearch_host = 'localhost'
elasticsearch_host = 'elasticsearch-dev'

# InfluxDB variables ##########################################################
#influxDB_host = 'localhost'
influxDB_host = 'influxdb-dev'
influxDB_port = '8086'
influxDB_username = 'root'
influxDB_password = 'root'


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.info("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
#    client.subscribe("$SYS/#")
    client.subscribe(mqtt_topic_libelium)
    client.subscribe(mqtt_topic_swisscom)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.info('MQTT topic;' + msg.topic)
    message_split = msg.topic.split('/')
    # Formatting required since Libelium MQTT topics and payload are fix
    if (message_split[0] =='Libelium'):
        customer = libelium_customer
        deviceType = message_split[0].lower()
        data = json.loads(libeliumXML2JSON(customer, deviceType, msg))
        write_storage(customer, deviceType, data)
    else:
        customer = message_split[1]
        deviceType = message_split[2]
        data = json.loads(msg.payload)
        write_storage(customer, deviceType, data)

# converting xml from libelium device to json
def libeliumXML2JSON(customer, deviceType, msg):
    root = ET.fromstring(str(msg.payload))
    data = {}
    data['customer'] = customer
    data['deviceType'] = deviceType
    data['deviceID'] = root[1].text
    data['sensor'] = root[7][9][0].text
    data['value'] = float(root[7][9][1].text)
    data['message_timestamp'] = root[2].text[:-7] + '.000'
    data['date'] = datetime.utcnow().isoformat()
    return json.dumps(data)

def write_storage(customer, device, payload):
    logger.info('MQTT payload;' + customer + ';' + device + ';' + payload['sensor'] + ';' + str(payload['value']))
    write_influxDB(customer, device, payload)
    write_elasticsearch(customer, device, payload)
    write_mongoDB(customer, device, payload)

# Write message to monogoDB
def write_mongoDB(database, collection, document):
    try:
        connection = MongoClient(mongodb_uri)
        db = connection[database][collection]
        result = db.insert(document)
        connection.close()
    except Exception, error:
        logger.info("Exception from write_mongoDB: " + str(error))
    else:
        #logger.info('MongoDB;' + database + ';' + collection + ';' + document['date'][:-5] + ';' + document['deviceID'] + ';' + document['sensor'])
        logger.info('write_mongoDB successfull')

def write_elasticsearch(customer, device, payload):
    try:
        es = Elasticsearch([{'host': elasticsearch_host}])
        res = es.index(index=customer, doc_type='mqtt_message', body=payload)
    except Exception, error:
        logger.info("Exception from write_elasticsearch: " + str(error))
    else:
        #logger.info('ElasticSearch;' + customer + ';' + device + ';' + payload['date'][:-5] + ';' + payload['deviceID'] + ';' + payload['sensor'] + ';' + str(res['created']))
        logger.info('write_elasticsearch successfull')

def write_influxDB(customer, device, payload):
    data = [{
        "measurement": device,
        "tags": {
                "customer": customer,
		"deviceType": payload['deviceType'],
                "deviceID": payload['deviceID'],
                "sensor": payload['sensor']
        },
        "time": payload['date'],
        "fields": {
                "value": payload['value']
            }
    }]
    try:
        client = InfluxDBClient(host = influxDB_host, port = influxDB_port, \
        username = influxDB_username, password = influxDB_password, database=customer)
        client.write_points(data)
    except Exception, error:
        logger.info("Exception from write_influxDB: " + str(error))
    else:
        #logger.info('InfluxDB;' + customer + ';' + device + ';' + payload['date'][:-5] + ';' + payload['deviceID'] + ';' + payload['sensor'])
        logger.info('write_influxDB successfull')

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(mqtt_host, mqtt_port, 60)
except Exception, error:
    logger.info("Exception from mqtt.client.connect: " + str(error))
else:
    logger.info('MQTT connected successfull')

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
