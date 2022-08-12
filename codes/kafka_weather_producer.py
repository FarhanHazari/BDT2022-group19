from pickle import TRUE
import sys
import time
from weather_data_generator import fetch_weather_data
#from kafka import KafkaProducer
from confluent_kafka import Producer

if __name__ == '__main__':
    # kafka Producer configuration
    topic = ['fcz61y67-weather_data']
    conf = {
            'bootstrap.servers':  'moped-01.srvs.cloudkafka.com:9094',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': "fcz61y67",
            'sasl.password': "3yYb9hzK2cifpPr5fRANXY7ICM0vFBW5"
            }

    # create the producer
    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            # sys.stderr.write('%% Message delivered to %s [%d]\n' %(msg.topic(), msg.partition()))
            sys.stderr.write('.')

    while TRUE:
        # Generate a kafka weather_data message
        weather_data_message = fetch_weather_data()

        print("Response: ", weather_data_message)
        # Send it to our 'fcz61y67-weather_data' topic
        # send message to topic in json format
        try:
            p.produce(topic[0], weather_data_message.encode('utf-8'), callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                len(p))
        
        p.poll(0)
        #sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
        p.flush()
        p.poll(0)

        # Sleep for a random number of seconds
        print('Weather message produced. . .')
        time_to_sleep = 300
        time.sleep(time_to_sleep)