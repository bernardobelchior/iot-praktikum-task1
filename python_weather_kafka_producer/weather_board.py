#!/usr/bin/python
import SI1132
import BME280
import sys
import time
import os
import json
import pytz
from gpio_button import GPIOButton

from kafka import KafkaProducer
from datetime import datetime

BUTTON_GPIO_PIN = 7
TIMEZONE = pytz.timezone('Europe/Berlin')

if len(sys.argv) != 3:
    print("Usage: weather_board.py <i2c device file> <kafka bootstrap server>")
    sys.exit()

si1132 = SI1132.SI1132(sys.argv[1])
bme280 = BME280.BME280(sys.argv[1], 0x03, 0x02, 0x02, 0x02)

producer_server = sys.argv[2].strip()

print "Connecting Kafka Producer to", producer_server

producer = KafkaProducer(bootstrap_servers=producer_server)


def get_altitude(pressure, seaLevel):
    atmospheric = pressure / 100.0
    return 44330.0 * (1.0 - pow(atmospheric/seaLevel, 0.1903))


def next_threshold(current_threshold):
    """Cycles between 10, 15, 20, 25 and 30 degrees Celsius"""
    return (current_threshold - 5) % 25 + 10


# Initial temperature threshold is 30 degrees Celsius, which will be changed afterwards to 10.
temp_threshold = 30


def on_button_press():
    global temp_threshold

    temp_threshold = next_threshold(temp_threshold)
    timestamp = datetime.now(TIMEZONE).isoformat()
    message = {'threshold': temp_threshold, 'timestamp': timestamp}
    print 'Temperature threshold set to', temp_threshold, 'C'
    producer.send('threshold_change', json.dumps(message).encode('utf-8'))

# Set the threshold to initial value
on_button_press()


button = GPIOButton(BUTTON_GPIO_PIN)
button.subscribe_button_pressed(on_button_press)

while True:
    # os.system('clear')
    # print "======== si1132 ========"
    # print "UV_index : %.2f" % (si1132.readUV() / 100.0)
    # print "Visible :", int(si1132.readVisible()), "Lux"
    # print "IR :", int(si1132.readIR()), "Lux"
    # print "======== bme280 ========"
    # print "temperature : %.2f 'C" % bme280.read_temperature()
    # print "humidity : %.2f %%" % bme280.read_humidity()
    # p = bme280.read_pressure()
    # print "pressure : %.2f hPa" % (p / 100.0)
    # print "altitude : %.2f m" % get_altitude(p, 1024.25)

    temperature = bme280.read_temperature()
    humidity = bme280.read_humidity()
    pressure = bme280.read_pressure()
    altitude = get_altitude(pressure, 1024.25)
    timestamp = datetime.now(TIMEZONE).isoformat()

    message = {'temperature': temperature, 'humidity': humidity,
               'pressure': pressure / 100.0, 'altitude': altitude, 'timestamp': timestamp}

    print "Current temperature:", temperature, "C"

    producer.send('measurements', json.dumps(message).encode('utf-8'))

    time.sleep(10)
