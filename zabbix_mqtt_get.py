#!/usr/bin/env python

from argparse import ArgumentParser
import time
from paho.mqtt.client import Client, MQTT_ERR_SUCCESS



parser = ArgumentParser()
parser.add_argument("-H", "--host", default="localhost")
parser.add_argument("-t", "--topic", default="$SYS/broker/version")
parser.add_argument("-T", "--timeout", type=float, default=30.0)
parser.add_argument("-v", "--verbose", action="store_true", default=False)
parser.add_argument("-n", "--max-messages", type=int, default=1)

args = parser.parse_args()


def on_message(client, userdata, message):
    userdata["messages_received"] += 1
    userdata["data"][message.topic] = message.payload
    if userdata["show_topics"]:
        print "{}: {}".format(message.topic, message.payload)
    else:
        print message.payload

user_data = {
    "show_topics": args.verbose,
    "time_start": time.time(),
    "timeout": args.timeout,
    "max_messages": args.max_messages,
    "messages_received":0,
    "data": {},
}

client = Client(userdata=user_data)
connect_result = client.connect(args.host)
client.loop()
if not connect_result == MQTT_ERR_SUCCESS:
    raise Exception("Client connection error {}".format(connect_result))

(subs_result, subs_id) = client.subscribe(args.topic)
client.on_message = on_message
if not subs_result == MQTT_ERR_SUCCESS:
    raise Exception("Subscription error {}".format(subs_result))

while True:
    client.loop()

    # check message limit
    if args.max_messages and (
            user_data["messages_received"] >= args.max_messages):

        client.disconnect()
        break

    # check time limit
    time_spent = time.time() - user_data["time_start"]
    if args.timeout and time_spent >= args.timeout:
        break
