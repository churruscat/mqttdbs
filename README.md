# mqttdbs

 Connects to a configured mqtt server, subscribes to the defined topics, updates the record, stores in a database (influxdb V1 or influxdb V2) and -if wanted- resends the record to another mqtt server.
 There is a configuration file /etc/mqttdbs/mqttdbs.conf to customize the behaviour.
 To get mqttdbs container, download it from churruscat/mqttdbs in docker hub. To run it, execute:

`docker run -d -v ~/mqttdbs:/etc/mqttdbs:rw --restart always --name mqttdbs churruscat/mqttdbs:latest `

If you are using docker-compose, add to your docker-compose.yaml file:
 `mqttdbs:
    container_name: mqttdbs
    image: churruscat/mqttdbs:latest
    volumes:
      - ~/mqttdbs:/etc/mqttdbs:rw
    depends_on:
      - mosquitto
      - influxdb
    restart: on-failure`

 ## configuration file mqttdbs.conf
`; configuration data of mqtt broker that mqttdbs reads. 
 [mqtt_broker_read]
	address=mosquitto
	userid=
	password=
	port=1883
	subscribe_topic= meteo/#, other/#

; configuration data of mqtt broker where mqttdbs sends data.
; if address s blank it will not send
[mqtt_broker_send]
	address=xx.yy.zz.aa
	userid=
	password=	
	port=1883
	publishTopic=cooked
; choose from debug, info, warting, error, critical
[log_level]
	log_level = info
; if using influxdb V1, configuration data
[influxdb_V1]
	address=xx.yy.zz.aa
	userid=
	password=	
	dbname=iotdb
; if using influxdb V2, configuration data	 
[influxdb_V2]
	org= MyOrg
	bucket=iotdb
	address=xx.yy.zz.aa
	token =xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`



