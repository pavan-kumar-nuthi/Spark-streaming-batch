import json
from pydoc_data.topics import topics
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import socket
consumer = KafkaConsumer('BTC-USD')



def process_messages():

        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        host="localhost"
        port=8083

        s.bind((host,port))
        print(f'Listening on port {port}')
        s.listen(5)

        c_s,c_addr=s.accept()
        print(f'Recieved connection from {str(c_addr)}')
        for msg in consumer:
                di=json.loads(msg.value)
                # print(type(di))
                di=json.loads(di)
                print(type(di))
                c_s.send(di["price"].encode('utf-8'))


process_messages()