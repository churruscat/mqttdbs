##!/usr/bin/env python
#chuRRuscat 2021 v 1.2
#     and, optional, send a message to an mqtt broker

import paho.mqtt.client as mqtt
import  json
import argparse

def mandaMsg(address,port,topic,payload):
        client = mqtt.Client(client_id='publisher')
        client.connect(address,port)
        msg=json.loads(payload)
        #client.publish(topic,json.loads(payload))
        client.publish(topic,(payload))


if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='mqttsend.py sends msgs to an qtt broker.')
        parser.add_argument('-t', '--topic', nargs='?', required=True,
                                                help='topic (message topic).')
        parser.add_argument('-p', '--payload', nargs='?', required=True,
                                                help='message payload.')
        parser.add_argument('-d', '--destport', nargs='?', required=False,default=1883,
                                                help='message payload.')
        parser.add_argument('-a', '--address', nargs='?', required=False,default='192.168.1.11',
                                                help='mqtt IP address.')
        args = parser.parse_args()
        mandaMsg(args.address,args.destport, args.topic,args.payload)

