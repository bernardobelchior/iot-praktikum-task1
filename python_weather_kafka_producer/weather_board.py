#!/usr/bin/python
import SI1132
import BME280
import sys
import time
import os
from kafka import KafkaProducer
import json

if len(sys.argv) != 3:
    print "Usage: weather_board.py <i2c device file> <kafka bootstrap server>"
    sys.exit()

si1132 = SI1132.SI1132(sys.argv[1])
bme280 = BME280.BME280(sys.argv[1], 0x03, 0x02, 0x02, 0x02)

producer_server = sys.argv[2].strip()

print "Connecting Kafka Producer to", producer_server

producer = KafkaProducer(bootstrap_servers=producer_server)

def get_altitude(pressure, seaLevel):
    atmospheric = pressure / 100.0
    return 44330.0 * (1.0 - pow(atmospheric/seaLevel, 0.1903))

while True:
    #os.system('clear')
#    print "======== si1132 ========"
#    print "UV_index : %.2f" % (si1132.readUV() / 100.0)
#    print "Visible :", int(si1132.readVisible()), "Lux"
#    print "IR :", int(si1132.readIR()), "Lux"
#    print "======== bme280 ========"
    #print "temperature : %.2f 'C" % bme280.read_temperature()
    #print "humidity : %.2f %%" % bme280.read_humidity()
    #p = bme280.read_pressure()
    #print "pressure : %.2f hPa" % (p / 100.0)
    #print "altitude : %.2f m" % get_altitude(p, 1024.25)

    temperature = bme280.read_temperature()
    humidity = bme280.read_humidity()
    pressure = bme280.read_pressure()
    altitude = get_altitude(pressure, 1024.25)

    message = { 'temperature': temperature, 'humidity': humidity, 'pressure': pressure / 100.0, 'altitude': altitude }

    print message
    
    producer.send('measurements', json.dumps(message).encode('utf-8'))

    time.sleep(15)
