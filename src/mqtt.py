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
    MODEL: ClassVar[Model] = Model(ModelFamily("tuzumkuru", "sensor"), "mqtt")
    
    # Broker parameters
    broker_address: str
    broker_port: int
    mqtt_topic: str
    mqtt_qos: int = 0

    latest_reading: str

    #Client
    mqtt_client: mqtt.Client
    mqtt_client = mqtt.Client()

    # Client Parameters
    # Get default values of parameters    
    client_id: str = mqtt_client._client_id  
    clean_session : bool = mqtt_client._clean_session
    protocol : int = mqtt_client._protocol # MQTTv31 = 3 MQTTv311 = 4 MQTTv5 = 5
    transport : str = mqtt_client._transport

    # enable_logger(logger=None) might be used to let paho use viam logger

    # username_pw_set(username, password=None)
    # set username and password if provided
    
    # Constructor
    @classmethod
    def new(cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]) -> Self:
        sensor = cls(config.name)
        sensor.reconfigure(config, dependencies)
        return sensor

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
        self.latest_reading = None

        # Get parameters from config
        self.broker_address = config.attributes.fields['broker_address'].string_value
        self.broker_port = int(config.attributes.fields['broker_port'].number_value)
        self.mqtt_topic = config.attributes.fields['mqtt_topic'].string_value
        
        
        # Set optional parameters if set in config
        self.mqtt_qos = int(config.attributes.fields['mqtt_qos'].number_value)
        self.client_id = config.attributes.fields['client_id'].string_value if 'client_id' in config.attributes.fields else self.client_id
        self.clean_session = bool(config.attributes.fields['clean_session'].bool_value)  if 'clean_session' in config.attributes.fields else self.clean_session
        self.protocol= config.attributes.fields['protocol'].string_value if 'protocol' in config.attributes.fields else self.protocol # MQTTv31 = 3 MQTTv311 = 4 MQTTv5 = 5
        self.transport = config.attributes.fields['transport'].string_value if 'transport' in config.attributes.fields else self.transport # tcp or websockets

        self.mqtt_client.reinitialise(self.client_id, self.clean_session)

        # Set up callbacks
        
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        # Connect to the broker
        self.mqtt_client.connect(self.broker_address, self.broker_port, 60)

        # Start the MQTT loop
        self.mqtt_client.loop_start()
        return


    def on_connect(self, client, userdata, flags, rc):
        LOGGER.info("Connected with result code "+str(rc))
        # Subscribe to the specified topic when connected
        client.subscribe(self.mqtt_topic, qos=self.mqtt_qos)


    def on_message(self, client, userdata, msg):
        LOGGER.info(f"MQTT Message Received: {str(msg)}")
        # Update the latest reading when a new message is received
        self.latest_reading = msg.payload.decode('utf-8')
    

    async def get_readings(self, *, extra: Optional[Mapping[str, Any]] = None, timeout: Optional[float] = None, **kwargs) -> Mapping[str, Any]:
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
        LOGGER.info("Test do_command: DONE!")


async def main():
    # Create an example ComponentConfig
    config = ComponentConfig()
    config.attributes.fields['broker_address'].string_value = "test.mosquitto.org"
    config.attributes.fields['broker_port'].number_value = 1883
    config.attributes.fields['mqtt_topic'].string_value = "my_test_topic"
    config.attributes.fields['clean_session'].bool_value = False
    config.attributes.fields['transport'].string_value = "websockets"
    config.attributes.fields['mqtt_qos'].number_value = 0

    # Create a sensor to test code
    my_mqtt_sensor=mqtt_sensor(name="mqtt-sensor")
    my_mqtt_sensor.validate(config)
    my_mqtt_sensor.reconfigure(config,None)

    time.sleep(2)
    signal = await my_mqtt_sensor.get_readings()
    print(signal)

if __name__ == '__main__':
    asyncio.run(main())