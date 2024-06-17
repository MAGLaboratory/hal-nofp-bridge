#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import signal, json, atexit, logging, socket, os
import traceback
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from enum import Enum
from typing import * 
from threading import Event

class NOFP(mqtt.Client):

    @dataclass_json
    @dataclass
    class config:
        name: str
        description: str
        mqtt_broker: str
        mqtt_port: int
        mqtt_timeout: int
        topic: str
        param_conv: List[Tuple[str, str]]
        mqtt_max_reconnects: Optional[int] = 10
        mqtt_max_startup: Optional[int] = 10
        mqtt_max_loop_reconnect: Optional = 10
        loglevel: Optional[str] = None

    # overloaded MQTT functions
    def on_log(self, client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_DEBUG:
            logging.debug("PAHO MQTT DEBUG: " + buf)
        elif level == mqtt.MQTT_LOG_INFO:
            logging.info("PAHO MQTT INFO: " + buf)
        elif level == mqtt.MQTT_LOG_NOTICE: 
            logging.warn("PAHO MQTT WARN: " + buf)
        elif level == mqtt.MQTT_LOG_WARNING:
            logging.warn("PAHO MQTT WARN: " + buf)
        else:
            logging.error("PAHO MQTT ERROR: " + buf)

    def on_connect(self, client, userdata, flags, rc):
        logging.info("MQTT Connected: " + str(rc))
        self.subscribe(self.config.topic)

    def on_message(self, client, userdata, message):
        if (message.topic == self.config.topic):
            try: 
                rcv = json.loads(message.payload.decode("utf-8"))
                for topic in self.config.param_conv:
                    val_f = float(rcv[topic[0]])
                    val_f *= 1000
                    data = format(val_f, ".0f")
                    self.publish(self.config.name+"/"+topic[1], data)
            except:
                logging.warn("Message failed to parse: " + str(message.topic) + " " + str(message.payload))
                logging.warn(traceback.format_exc())

    def on_disconnect(self, client, userdata, rc):
        logging.warning("MQTT Disconnected: " + str(rc))
        if rc != 0:
            logging.error("Unexpected disconnection.  Attempting reconnection.")
            reconnect_count = 0
            while (reconnect_count < self.config.mqtt_max_reconnects):
                try:
                    reconnect_count += 1
                    self.reconnect()
                    break
                except OSError:
                    logging.error("Connection error while trying to reconnect.")
                    logging.error(traceback.format_exc())
                    self.tEvent.wait(30)
            if (reconnect_count >= self.config.mqtt_max_reconnects):
                logging.critical("Too many reconnect tries.  Exiting.")
                exit(1)

    # our custom functions
    def signal_handler(self, signum, frame):
        logging.warning("Caught a deadly signal: " + str(signum) + "!")
        self.running = False

    def run(self):
        self.tEvent = Event()
        self.running = True
        startup_count = 0
        self.modbus_check_count = 0
        self.loop_count = 0
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        try:
            if type(logging.getLevelName(self.config.loglevel.upper())) is int:
                logging.basicConfig(level=self.config.loglevel.upper())
            else:
                logging.warning("Log level not configured.  Defaulting to WARNING.")
        except (KeyError, AttributeError) as e:
            logging.warning("Log level not configured.  Defaulting to WARNING.  Caught: " + str(e))
        if self.config.mqtt_max_loop_reconnect < 1:
            logging.critical("MQTT maximum reconnect tries can not be less than 1.")
            exit(1)
        if self.config.mqtt_max_startup < 1:
            logging.critical("MQTT maximum startup count can not be less than 1.")
            exit(1)
        if self.config.mqtt_max_reconnects < 1:
            logging.critical("MQTT maximum reconnect count can not be less than 1.")
            exit(1)

        while startup_count < self.config.mqtt_max_startup:
            try:
                startup_count += 1
                self.connect(self.config.mqtt_broker, self.config.mqtt_port, self.config.mqtt_timeout)
                atexit.register(self.disconnect)
                break
            except OSError:
                logging.error("Error connecting on bootup.")
                logging.error(traceback.format_exc())
                self.tEvent.wait(30)

        if startup_count >= self.config.mqtt_max_startup:
            logging.critical("Too many startup tries. Exiting after try " + str(self.config.mqtt_max_startup) + ".")
            exit(1)

        logging.info("Startup success.")
        reconnect_flag = False
        loop_reconnect_count = 0
        while self.running and (loop_reconnect_count < self.config.mqtt_max_loop_reconnect):
            if self.loop_count >= 65535:
                self.loop_count = 0
            else:
                self.loop_count += 1

            try:
                if reconnect_flag == True:
                    self.reconnect()
                    self.reconnect_flag = False

                self.loop()
                # reset the reconnect counter if our last was successful
                loop_reconnect_count = 0
            except SystemExit:
                # exiting. stop the loop.
                break
            except (socket.timeout, TimeoutError, ConnectionError):
                loop_reconnect_count += 1
                reconnect_flag = True
                logging.error("MQTT loop error.  Attempting to reconnect. " + loop_reconnect_count + " of " + self.config.mqtt_max_loop_reconnect)
            except:
                logging.logging.critical("Exception in MQTT loop.")
                logging.critical(traceback.format_exc())
                logging.critical("Exiting.")
                exit(2)
        if loop_reconnect_count >= self.config.mqtt_max_loop_reconnect:
            logging.critical("Too many loop reconnects.  Exiting.")
            exit(1)
        logging.info("Successfully reached exit.")
        exit(0)

# code that is run on direct invocation of this file
if __name__ == "__main__":
    nofp = NOFP()
    file_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(file_path, "hal-nofp-bridge.json"), "r") as configFile:
        nofp.config = NOFP.config.from_json(configFile.read())
    nofp.run()


