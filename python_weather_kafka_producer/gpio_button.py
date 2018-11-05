import wiringpi as wpi
import time
import threading

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
