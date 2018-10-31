#!/usr/bin/python
import SI1132
import BME280
import threading
import wiringpi as wpi
import sys
import time
import os
import json
import pytz

from kafka import KafkaProducer
from datetime import datetime

BUTTON_GPIO_PIN=7

if len(sys.argv) != 3:
    print "Usage: weather_board.py <i2c device file> <kafka bootstrap server>"
    sys.exit()

si1132 = SI1132.SI1132(sys.argv[1])
bme280 = BME280.BME280(sys.argv[1], 0x03, 0x02, 0x02, 0x02)
wpi.wiringPiSetup()

class GPIOButton:
    def __init__(self, gpio_pin):
      self.pin = gpio_pin

      wpi.pinMode(self.pin, 0)

      self.last_result = wpi.digitalRead(self.pin)
      self.thread = threading.Thread(target=self.__event_loop)

      # A daemon thread will die when the main thread is killed (https://stackoverflow.com/a/2564282/7644138)
      self.thread.daemon = True

      self.thread.start()

    def __event_loop(self):
      while True:
          time.sleep(0.1)
          self.result = wpi.digitalRead(self.pin)
      
	  # Button pushed
          if self.last_result == 1 and self.result == 0 and self.callback:
	      self.callback()
      
	  # Button released
          # if self.last_result == 0 and self.result == 1:
          #     print("Button released")
      
          self.last_result = self.result
       

    # Blocks the program
    def subscribe_button_pressed(self, callback):
      self.callback = callback

producer_server = sys.argv[2].strip()

print "Connecting Kafka Producer to", producer_server

producer = KafkaProducer(bootstrap_servers=producer_server)

def get_altitude(pressure, seaLevel):
    atmospheric = pressure / 100.0
    return 44330.0 * (1.0 - pow(atmospheric/seaLevel, 0.1903))

def on_button_press():
    print("Button pressed")

timezone = pytz.timezone('Europe/Berlin')
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
    timestamp = datetime.now(timezone).isoformat()

    message = { 'temperature': temperature, 'humidity': humidity, 'pressure': pressure / 100.0, 'altitude': altitude, 'timestamp': timestamp }

    print message
    
    producer.send('measurements', json.dumps(message).encode('utf-8'))

    time.sleep(15)
