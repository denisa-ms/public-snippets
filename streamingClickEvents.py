import datetime
import time
import random
import socket
import struct
import json
from azure.eventhub import EventHubProducerClient, EventData
import psutil
import pyodbc
import pandas as pd

eventHubConnString = "<paste here the events hub shared access signature primary key>"
eventHubName = "<paste here the events hub>"
producer = EventHubProducerClient.from_connection_string(conn_str=eventHubConnString, eventhub_name=eventHubName)
hostname = socket.gethostname()

def generateRandomIP():
    ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    return ip

def getRandomBrowser():
    browsersList = ["Edge", "Chrome", "Safari", "Firefox"]
    browser = random.choice(browsersList)
    return browser

def getRandomMethod():
    methods = [ "GET", "POST", "PUT", "DELETE" ]
    method = random.choice(methods)
    return method

def getRandomURL():
    urls = ["/page1.html", "/page2.html", "/page3.html", "/page4.html", "/page5.html" ,"/page6.html"  ]
    url = random.choice(urls)
    return url

def getCurrentDate():
    today = datetime.datetime.now()
    current = today.strftime("%Y-%m-%d")
    return current

def getCurrentTime():
    today = datetime.datetime.now()
    current = today.strftime("%H:%M:%S")
    return current

def getRandomTemperature():
    return random.randint(0,99)

def generateClickEvent():
    click_event = {
        "tableName" : "clicks",
        "date" : getCurrentDate(),
        "time": getCurrentTime(),
        "ip": generateRandomIP(),
        "browser": getRandomBrowser(),
        "method": getRandomMethod(),
        "url" : getRandomURL(),
        "productID": getRandomProductId()
    }
    return click_event

def getRandomProductId():
    return str(random.randint(0,99999))

def sendToEventsHub(jsonEvent):
    eventString = json.dumps(jsonEvent)
    print(eventString) 

    event_data_batch = producer.create_batch() 
    event_data_batch.add(EventData(eventString)) 
    producer.send_batch(event_data_batch)

def generateClicks():
    try:
        while True:
            clickEvent = generateClickEvent()    
            sendToEventsHub(clickEvent)
            # time.sleep(10)

    except KeyboardInterrupt:
        producer.close()
        conn.close()

if __name__ == "__main__":
    generateClicks()
