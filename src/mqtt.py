import json
from typing import ClassVar, Mapping, Sequence, Any, Dict, Optional, Tuple, Final, List, cast
from typing_extensions import Self

from typing import Any, Mapping, Optional






from viam.module.types import Reconfigurable
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName, Vector3
from viam.resource.base import ResourceBase
from viam.resource.types import Model, ModelFamily

from viam.components.sensor import Sensor
from viam.logging import getLogger

import time
import asyncio

import paho.mqtt.client as mqtt
from viam.components.component_base import ValueTypes



LOGGER = getLogger(__name__)



class mqtt_sensor(Sensor, Reconfigurable):
    
    """
    Sensor represents a physical sensing device that can provide measurement readings.
    """
    

    MODEL: ClassVar[Model] = Model(ModelFamily("tuzumkuru", "sensor"), "mqtt")
    
    # create any class parameters here, 'some_pin' is used as an example (change/add as needed)
    some_pin: int

    def  __init__(self, name, *args):
        print("__init__ called")
        self.name = name
        # Set your MQTT broker's information
        self.broker_address = "test.mosquitto.org"
        self.port = 1883
        self.topic = "my_test_topic"
        self.latest_reading = None

        # Create an MQTT client instance
        self.client = mqtt.Client()

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Connect to the broker
        self.client.connect(self.broker_address, self.port, 60)

        # Start the MQTT loop
        self.client.loop_start()
        
    # Constructor
    @classmethod
    def new(cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]) -> Self:
        my_class = cls(config.name)
        my_class.reconfigure(config, dependencies)
        return my_class

    # Validates JSON Configuration
    @classmethod
    def validate(cls, config: ComponentConfig):
        # here we validate config, the following is just an example and should be updated as needed
        some_pin = config.attributes.fields["some_pin"].number_value
        if some_pin == "":
            raise Exception("A some_pin must be defined")
        return

    # Handles attribute reconfiguration
    def reconfigure(self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]):
        # here we initialize the resource instance, the following is just an example and should be updated as needed
        self.some_pin = int(config.attributes.fields["some_pin"].number_value)
        return

    """ Implement the methods the Viam RDK defines for the Sensor API (rdk:component:sensor) """
    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))

        # Subscribe to the specified topic when connected
        client.subscribe(self.topic)

    def on_message(self, client, userdata, msg):
        print("on_message called")
        # Update the latest reading when a new message is received
        self.latest_reading = msg.payload.decode('utf-8')
 
        print(self.latest_reading)
    
    async def get_readings(
        self, *, extra: Optional[Mapping[str, Any]] = None, timeout: Optional[float] = None, **kwargs
    ) -> Mapping[str, Any]:
        """
        Obtain the measurements/data specific to this sensor.

        Returns:
            Mapping[str, Any]: The measurements. Can be of any type.
        """
        ...
         # Replace mqtt_data with self.latest_reading in the following code
        if self.latest_reading is None:
            return {"status": "No data"}

        if isinstance(self.latest_reading, str):
            try:
                json_data = json.loads(self.latest_reading)
                if isinstance(json_data, dict):
                    return json_data
                else:
                    return {"data": json_data}
            except json.JSONDecodeError:
                return {"data": self.latest_reading}

        if isinstance(self.latest_reading, int):
            return {"data": self.latest_reading}

        # If it's already a dictionary, return it
        if isinstance(self.latest_reading, Mapping):
            return self.latest_reading

        # For any other type, convert it to a string and return
        return {"data": str(self.latest_reading)}



    
    async def do_command(self, command: Mapping[str, ValueTypes], *, timeout: Optional[float] = None, **kwargs) -> Mapping[str, ValueTypes]:
        print("Done!")



async def main():
    my_mqtt_sensor=mqtt_sensor(name="mqtt-sensor")
    time.sleep(2)
    signal = await my_mqtt_sensor.get_readings()
    print(signal)

if __name__ == '__main__':
    asyncio.run(main())