#!/usr/bin/env python
#chuRRuscat 2021 v 1.2
# read an mqtt broker, 
#        Save into an influxdb database
#        and, optional, resend the message to another mqtt broker
#read from mqttdbs.conf, :

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import influxdb.exceptions
import  json, math
from datetime import datetime
from time import time, altzone ,sleep
import os, socket, sys, subprocess, logging
import configparser

configFile='/etc/mqttdbs/mqttdbs.conf'  # linux or docker
dbport=8086
dbserver="influxdb"
dbname="iotdb"
dbuser=''
dbpassword=''
mqttbroker="mosquitto"
tipoLogging=['none','debug', 'info', 'warning', 'error' ,'critical']
clientes={
	"reader":{"clientId":"c_reader","broker":"127.0.0.1","port":1883,"name":"blank",
			  "cliente":"c_reader_mqttdbs","userid":"","password":"",
			  "subscribe_topic":["meteo/#","cooked"],"publish_topic":"cooked", "activo":True},
	#I will use reader2 to resend messages when there are problems storing records          
	   
	"sender":{"clientId":"c_sender","broker":"","port":1883,"name":"blank",
			  "cliente":"","userid":"","password":"",
			  "publish_topic":"cooked",
			  "activo":False},
}

def db_insert(body):
	response=False
	try:
		client = InfluxDBClient(dbserver, dbport, dbuser, dbpassword, dbname)
		logging.info("connected to database")
	except:
		logging.warning("error connecting to database")
		logging.warning("host. %s \t port: %s \t, user:%s\t, password:%s\t, dbname:%s",dbserver, dbport, dbuser, dbpassword, dbname)
		sleep(60)
		return False
	body1=json.loads(body)  # ???? dumps?  hago loads y luego dumps
	punto=json.loads('['+json.dumps(body1)+']')
	#?punto=json.loads('['+body+']')
	try:
		for clave in body1['fields'].copy():
			if (math.isnan(float( body1['fields'][clave]))):
				logging.warning("must delete : "+str(clave)+' = '+str(body1['fields'][clave]))
				del body1['fields'][clave]
			punto=json.loads('['+json.dumps(body1)+']')        
	except:
		logging.warning("error en registro: "+str(body))
		return True  #if record has an error, I have to discard it
	try:
		response=client.write_points(punto)
		logging.warning("Record stored:    "+str(response)+' ->'+str(punto))
		client.close()
		return True
	except influxdb.exceptions.InfluxDBClientError as err:
		logging.warning("record discarded :"+str(response)+' ->'+str(punto))
		logging.warning(" Client Error: "+ str(err))
		client.close()
		return True  #if here care issues onclient side, I discard the record
	except influxdb.exceptions.InfluxDBServerError as err:
		logging.warning("record discarded :"+str(response)+' ->'+str(punto))
		logging.warning("Server Error: "+ str(err))
		sleep(60)
		return False	# a valid record could not be stored, I'll retry
	except :
		logging.warning("record discarded :"+str(response)+' ->'+str(punto))
		logging.warning("Database not available: ")
		sleep(60)
		return False		

# Funciones de Callback
def on_connect(mqttCliente, userdata, flags, rc):
	logging.info("Connected to broker")
 
def on_subscribe(mqttCliente, userdata, mid, granted_qos):
	logging.info("Subscribed OK; message "+str(mid)+"   qos= "+ str(granted_qos))
	sleep(1)

def on_disconnect(mqttCliente, userdata, rc):
	logging.info("Disconnected, rc= "+str(rc))    
	reconectate(mqttCliente)   

def on_publish(mqttCliente, userdata, mid):
	logging.info("message published "+ str(mid))   

def reconectate(mqttCliente):
	conectado=False
	while (not conectado):
		try:
			logging.info("reconnect  " )
			mqttCliente.reconnect()
			conectado=True
			sleep(2)
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Connection error 1 = "+ exErr.message)
			else:
				logging.warning("Connection error 2 = "+exErr)     
			sleep(30)
#Initializes an mqtt Client
def arrancaCliente(senderStruct, cleanSess):
		#global clientes
		if (cleanSess==False):
			senderStruct["cliente"] = mqtt.Client(senderStruct["clientId"],clean_session=cleanSess)
		else:
			senderStruct["cliente"] = mqtt.Client(clean_session=cleanSess)      
		senderStruct["cliente"].on_message = on_message
		senderStruct["cliente"].on_connect = on_connect
		senderStruct["cliente"].on_publish = on_publish
		senderStruct["cliente"].on_subscribe = on_subscribe

		if (senderStruct["userid"]!=''):
			senderStruct["cliente"].username_pw_set(senderStruct["userid"] , password=senderStruct["password"])
		senderStruct["cliente"].connect(senderStruct["broker"],senderStruct["port"])
		senderStruct["cliente"].reconnect_delay_set(60, 600) 
		print(senderStruct["subscribe_topic"])
		senderStruct["cliente"].subscribe(senderStruct["subscribe_topic"])
		logging.info(senderStruct)
	
def on_message(mqttCliente, userdata, message):
	global clientes
	#if (message.topic.split('/')[0] == 'zigbee2mqtt'):
	#	return	
	logging.info("Topic :"+  str(message.topic))
	if (message.topic==clientes["sender"]["publish_topic"]):
		crudo=False
		logging.info("Comes from a proxy")			
	else:
		crudo=True	
		logging.info("Comes directly from a sensor")
	#If it comes directly fom a sensor, add meassurement and time
	if crudo:
		measurement=message.topic.split('/')[0]
		logging.info("crudo: "+measurement)
		secs,usecs=divmod(time(),1)
		if (usecs==0):
			usecs=1e-9
		while (usecs<0.1):
			usecs=usecs*10
		try:
			payload=json.loads(message.payload.decode()) 
		except:
			logging.warning("Could jsonize: "+message.payload.decode())
			return
		dato='{"measurement":"'+measurement+'","time":'+str(int(secs))+str(int(usecs*1e9))+',"fields":'+json.dumps(payload[0])+',"tags":'+json.dumps(payload[1])+'}'
		logging.info("dato: "+dato)		
	else :
		logging.info(message.payload)
		dato=json.loads(message.payload)
	logging.info(dato)
	logging.info("salva en influxdb")
	if(db_insert(dato)==False):   #if I can't store record, I resend it "cooked" to mqtt queue
		logging.info("Record not stored will re-queue it")
		try:  
			result, mid = clientes["sender"]["cliente"].publish(clientes["sender"]["publish_topic"], json.dumps(dato), 1, True )
			logging.info("sent by reader2 "+str(result))
		except:
			logging.warning("Connection error to reader2 = "+exErr)				   
			sleep(60)
			arrancaCliente(clientes["reader"],False)		
	if len(clientes["sender"]["broker"])>2:
		logging.info("prepare to send to a remote qmtt broker")
		try:   
			result, mid = clientes["sender"]["cliente"].publish(clientes["sender"]["publish_topic"], json.dumps(dato), 1, True )
			logging.info("sent rc="+str(result))
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Connection error type 1 = "+ exErr.message)
			else:
				logging.warning("Connection error type 2 = "+ exErr)				   
			sleep(30)
			arrancaCliente(clientes["sender"],False)
	return

if __name__ == '__main__':
	parser = configparser.ConfigParser()
	parser.read(configFile)
	if parser.has_section("log_level"):
		if parser.has_option("log_level","log_level"):	
			loglevel=parser.get("log_level","log_level")
		else:
			loglevel='warning'
	else: 
		loglevel='warning'
	logging.basicConfig(stream=sys.stderr, format = '%(asctime)-15s  %(message)s', level=loglevel.upper())	
	if parser.has_section("mqtt_broker_send"):
		if parser.has_option("mqtt_broker_send","address"):
			clientes["sender"]["broker"]=parser.get("mqtt_broker_send","address")
		if parser.has_option("mqtt_broker_send","port"):
			clientes["sender"]["port"]=int(parser.get("mqtt_broker_send","port"))
		if parser.has_option("mqtt_broker_send","userid"):
			clientes["sender"]["userid"]=parser.get("mqtt_broker_send","userid")
		if parser.has_option("mqtt_broker_send","password"):
			clientes["sender"]["password"]=parser.get("mqtt_broker_send","password")
		if parser.has_option("mqtt_broker_send","publish_topic"):
			clientes["sender"]["publish_topic"]=parser.get("mqtt_broker_send","publish_topic")
			clientes["reader"]["publish_topic"]=parser.get("mqtt_broker_send","publish_topic")  #to resend in case of failure
	if parser.has_section("mqtt_broker_read"):
		if parser.has_option("mqtt_broker_read","address"):
			clientes["reader"]["broker"]=parser.get("mqtt_broker_read","address")
		if parser.has_option("mqtt_broker_read","port"):
			clientes["reader"]["port"]=int(parser.get("mqtt_broker_read","port"))
		if parser.has_option("mqtt_broker_read","userid"):
			clientes["reader"]["userid"]=parser.get("mqtt_broker_read","userid")
		if parser.has_option("mqtt_broker_read","password"):
			clientes["reader"]["password"]=parser.get("mqtt_broker_read","password")
		if parser.has_option("mqtt_broker_read","subscribe_topic"):
			#clientes["reader"]["subscribe_topic"]=parser.get("mqtt_broker_read","subscribe_topic")
			subscribe=parser.get("mqtt_broker_read","subscribe_topic").split(",")
			clientes["reader"]["subscribe_topic"]=[]
			for i in range(0,len(subscribe)) :
				topic1=(subscribe[i].strip(),1)
				logging.info(topic1)
				clientes["reader"]["subscribe_topic"].append(topic1)
				logging.info("subscribe topic="+str(clientes["reader"]["subscribe_topic"]))
			topic1=(clientes["reader"]["publish_topic"],1)
			clientes["reader"]["subscribe_topic"].append(topic1)
	
	if parser.has_section("database"):
		if parser.has_option("database","address"):
			dbserver=parser.get("database","address")
		if parser.has_option("database","dbname"):
			dbname=parser.get("database","dbname")
		if parser.has_option("database","userid"):
			dbuser=parser.get("database","userid")
		if parser.has_option("database","password"):
			dbpassword=parser.get("database","password")                        
	logging.info("IP addr: "+dbserver)
	logging.info("Reader: "+str(clientes["reader"]))
	logging.info("Sender: "+str(clientes["sender"]))

	## Define mqtt reader
	logging.info(clientes["reader"])
	arrancaCliente(clientes["reader"],True)
	logging.info("READER")
	logging.info(clientes["reader"]["subscribe_topic"])
	logging.info(clientes["reader"])
	#and, if configured, client gateway that will resend messages to remote queue
	clientes["reader"]["cliente"].loop_start()                 #start the loop
	if (clientes["sender"]["broker"]!=''):
		arrancando=True
		while (arrancando):
			try:
				arrancaCliente(clientes["sender"],True)
				arrancando=False
			except:
				logging.warning("could not connect to sender: "+clientes["sender"]["broker"])
				sleep(60)

	try:
		while True:
			sleep(5)
	except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
		print("\nKilling Thread...")
		clientes["reader"]["cliente"].loop_stop()                 #start the loop
	print("Done.\nExiting.")