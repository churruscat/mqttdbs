#!/usr/bin/env python
#by chuRRuscat@morrastronix 
# read an mqtt broker, 
#        Save into an influxdb database
#        and, optional, resend the message to another mqtt broker
#read from mqttdbs.conf, :
#  July 2021 v1.3 added support for influxdb version 1.x and 2.x

import paho.mqtt.client as mqtt
import influxdb_client
import influxdb
import influxdb.exceptions
import  json, math
from datetime import datetime
from time import time, altzone ,sleep
import sys
import logging
import configparser

configFile='/etc/mqttdbs/mqttdbs.conf'  # linux or docker
dbport=8086
dbserver="influxdb"
dbname="iotdb"
dbuser=''
dbpassword=''
tipoLogging=['none','debug', 'info', 'warning', 'error' ,'critical']
clientes={
    "reader":{"Cliente_name":"c_reader","broker":"127.0.0.1","port":1883,"name":"Reader",
              "Cliente":"Reader","userid":"","password":"",
              "subscribe_topic":["meteo/envia"],"publish_topic":"cooked", "activo":True},
    #I will use reader2 to resend messages when there are problems storing records          
       
    "sender":{"Cliente_name":"c_sender","broker":"","port":1883,"name":"Sender",
              "Cliente":"Sender","userid":"","password":"",
              "publish_topic":"cooked","subscribe_topic":[("Do_not",1)],
              "activo":False},
    }

def db_insert(dbversion,body):
    response=False
    logging.debug("raw payload to dbinsert: "+body)
    try:
        body1=json.loads(body)  # ???? dumps?  hago loads y luego dumps
    except:
        body1=body
        logging.warning("unexpected format :"+body)
    try:
        for clave in body1['fields'].copy():
            logging.debug("Exploro clave: %s",clave)
            if (math.isnan(float( body1['fields'][clave]))):
                logging.warning("must delete : "+str(clave)+' = '+str(body1['fields'][clave]))
                del body1['fields'][clave]
    except:
        logging.warning("error en registro: "+str(body))
        return True  #if record has an error, I have to discard it
   
    logging.debug("db_version= %s",dbversion)
    if dbversion==1 :
        try:
            punto=json.loads('['+json.dumps(body1)+']')            
            logging.debug("Conecting to database: %s: %s, user:%s , dbname:%s",dbserver, dbport, dbuser, dbname)
            client = influxdb.InfluxDBClient(dbserver, dbport, dbuser, dbpassword, dbname)
            logging.debug("connected to database")
        except:
            logging.warning("error connecting to database")
            logging.warning("host. %s  port: %s , user:%s, password:%s, dbname:%s",dbserver, dbport, dbuser, dbpassword, dbname)
            sleep(60)
            return False        
        try:
            response=client.write_points(punto)
            logging.info("Record stored:    "+str(response)+' ->'+str(punto))
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
            sleep(5)
            return False    # a valid record could not be stored, I'll retry
        except :
            logging.warning("record discarded :"+str(response)+' ->'+str(punto))
            logging.warning("unknown warning: ")
            sleep(5)
            return False    
    elif dbversion==2 :
        try:
            punto=json.loads(json.dumps(body1))
            logging.debug(punto)                                      
            logging.debug("connecting to influxdb V2 Database. url. %s, org : %s  bucket: %s",dbserver, org, bucket)
            client = influxdb_client.InfluxDBClient(url=dbserver, token=token, org=org) 
            logging.debug(client.ready)
            logging.debug("connected to influxdb V2 Database.")         
            write_api = client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)
            logging.debug("write API prepared")
            logging.debug(write_api)
        except:
            logging.warning("error connecting to database")
            logging.warning("host: %s  , org: %s, bucket:%s \n token: %s",dbserver, org, bucket, token)
            sleep(60)
            return False        
        try:
            write_api.write(bucket, org, punto) 
            logging.info("Record stored:  "+ str(punto))
            return True
        except influxdb.exceptions.InfluxDBClientError as err:
            logging.warning("record discarded :"+str(response)+' ->'+str(punto))
            logging.warning(" Client Error: "+ str(err))
            return True  #if here care issues onclient side, I discard the record
        except influxdb.exceptions.InfluxDBServerError as err:
            logging.warning("record discarded :"+str(response)+' ->'+str(punto))
            logging.warning("Server Error: "+ str(err))
            logging.warning("host: %s , org:%s ,bucket:%s \n token: %s",dbserver, org,  bucket, token)
            sleep(60)
            return False    # a valid record could not be stored, I'll retry
        except :
            logging.warning("record discarded :"+str(response)+' ->'+str(punto))
            logging.warning("Undetermined error: ")
            logging.warning("host: %s , org:%s ,bucket:%s \n token: %s",dbserver, org,  bucket, token)
            return True 

# Funciones de Callback
def on_connect(mqttCliente, userdata, flags, rc):
    #logging.info("Connected to broker")
    pass
 
def on_subscribe(mqttCliente, userdata, mid, granted_qos):
    logging.info("Subscribed OK; message "+str(mid)+"   qos= "+ str(granted_qos))
    sleep(1)

def on_disconnect(mqttCliente, userdata, rc):
    logging.info("Disconnected, rc= "+str(rc))    
    reconectate(mqttCliente)   

def on_publish(mqttCliente, userdata, mid):
    logging.debug("message published "+ str(mid))   

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
def arrancaCliente(mqttStruct , cleanSess):
        #global clientes
        if (cleanSess==False):
            mqttStruct["Cliente"] = mqtt.Client(mqttStruct ["Cliente_name"],clean_session=cleanSess)
        else:
            mqttStruct["Cliente"] = mqtt.Client(clean_session=cleanSess)      
        mqttStruct["Cliente"].on_message = on_message
        mqttStruct["Cliente"].on_connect = on_connect
        mqttStruct["Cliente"].on_publish = on_publish
        mqttStruct["Cliente"].on_subscribe = on_subscribe
        logging.info("call back registered for "+mqttStruct["Cliente_name"])
        if (mqttStruct["userid"]!=''):
            mqttStruct["Cliente"].username_pw_set(mqttStruct ["userid"] , password=mqttStruct ["password"])
        if (len(mqttStruct["broker"])<2):
            logging.info("No resending messages")
        else:             
            mqttStruct["Cliente"].connect(mqttStruct["broker"],mqttStruct ["port"])
            mqttStruct["Cliente"].reconnect_delay_set(1, 120)
        logging.info("Connected, now subscribe  "+mqttStruct["Cliente_name"]+" topics ")
        logging.info(mqttStruct["subscribe_topic"])
        if (mqttStruct["name"]!="Sender"):
            mqttStruct["Cliente"].subscribe(mqttStruct["subscribe_topic"])
            logging.debug("Subscribe Reader")            
        else:
            logging.debug("do not subscribe Sender")
    
def on_message(mqttCliente, userdata, message):
    global clientes
    logging.debug("Topic :"+  str(message.topic))
    if (message.topic==clientes["sender"]["publish_topic"]):
        crudo=False
        logging.debug("Comes from a proxy")         
    else:
        crudo=True  
        logging.debug("Comes directly from a sensor")
    #If it comes directly fom a sensor, add meassurement and time
    if crudo:
        measurement=message.topic.split('/')[0]
        logging.debug("crudo: "+measurement)
        secs,usecs=divmod(time(),1)
        if (usecs==0):
            usecs=1e-9
        while (usecs<0.1):
            usecs=usecs*10
        try:
            payload=json.loads(message.payload.decode()) 
            dato='{"measurement":"'+measurement+'","time":'+str(int(secs))+str(int(usecs*1e9))+',"fields":'+json.dumps(payload[0])+',"tags":'+json.dumps(payload[1])+'}'
        except:
            logging.warning("Could not jsonize: "+message.payload.decode())
            return
    else :
        logging.debug(message.payload)
        dato=json.loads(message.payload)
    logging.debug("dato: "+dato)     
    logging.debug("salva en influxdb")
    if(db_insert(dbversion,dato)==False):   #if I can't store record, I resend it "cooked" to mqtt queue
        logging.warning("Record not stored will re-queue it")
        try:  
            result, mid = clientes["reader"]["Cliente"].publish(clientes["sender"]["publish_topic"], json.dumps(dato), 1, True )
            logging.info("Record requeued "+str(result))
        except:
            logging.warning("Connection error when requeing = "+exErr)                
            sleep(60)
            #arrancaCliente(clientes["reader"],False)        
    if len(clientes["sender"]["broker"])>2:
        try:   
            result, mid = clientes["sender"]["Cliente"].publish(clientes["sender"]["publish_topic"], json.dumps(dato), 1, True )
            logging.info("sent to  " +clientes["sender"]["broker"]+"  rc="+str(result))
        except Exception as exErr:
            if hasattr(exErr, 'message'):
                logging.warning("Connection error type 1 = "+ exErr.message)
            else:
                logging.warning("Connection error type 2 = "+ exErr)                   
            sleep(30)
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
            subscribe=parser.get("mqtt_broker_read","subscribe_topic").split(",")
            clientes["reader"]["subscribe_topic"]=[]
            for i in range(0,len(subscribe)) :
                topic1=(subscribe[i].strip(),1)
                logging.debug(topic1)
                clientes["reader"]["subscribe_topic"].append(topic1)
                logging.info("subscribe topic="+str(clientes["reader"]["subscribe_topic"]))
            topic1=(clientes["reader"]["publish_topic"],1)
            clientes["reader"]["subscribe_topic"].append(topic1)
    
    if parser.has_section("influxdb_V1"):
        dbversion=1
        if parser.has_option("influxdb_V1","address"):
            dbserver=parser.get("influxdb_V1","address")
        if parser.has_option("influxdb_V1","dbname"):
            dbname=parser.get("influxdb_V1","dbname")
        if parser.has_option("influxdb_V1","userid"):
            dbuser=parser.get("influxdb_V1","userid")
        if parser.has_option("influxdb_V1","password"):
            dbpassword=parser.get("influxdb_V1","password")      

    if parser.has_section("influxdb_V2"):
        dbversion=2
        if parser.has_option("influxdb_V2","address"):
            dbserver=parser.get("influxdb_V2","address")
            dbserver="http://"+dbserver+":"+str(dbport)
        if parser.has_option("influxdb_V2","bucket"):
            bucket=parser.get("influxdb_V2","bucket")
        if parser.has_option("influxdb_V2","org"):
            org=parser.get("influxdb_V2","org")         
        if parser.has_option("influxdb_V2","token"):
            token=parser.get("influxdb_V2","token")
    logging.info("dbversion: "+str(dbversion))      
    sleep(30)
    ## Define mqtt reader
    arrancaCliente(clientes["reader"],False)
    logging.info("READER")
    logging.info(clientes["reader"])
    logging.info("SENDER")
    logging.info(clientes["sender"])    
    #and, if configured, client gateway that will resend messages to remote queue
    clientes["reader"]["Cliente"].loop_start()                 #start the loop

    if (clientes["sender"]["broker"]!=''):
        arrancando=True
        logging.info("starting sender to %s",clientes["sender"]["broker"])
        while (arrancando):
            try:
                arrancaCliente(clientes["sender"],True)
                clientes["sender"]["Cliente"].loop_start()                 #start the loop
                arrancando=False
            except:
                logging.warning("could not connect to remote: "+clientes["sender"]["broker"])
                sleep(60)

    try:
        while True:
            sleep(5)
    except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
        print("\nKilling Thread...")
    print("Done.\nExiting.")
