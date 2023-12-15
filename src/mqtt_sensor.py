import json
import asyncio
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
import paho.mqtt.client as mqtt

LOGGER = getLogger(__name__)


class mqtt_sensor(Sensor, Reconfigurable):
    MODEL: ClassVar[Model] = Model(ModelFamily("tuzumkuru", "sensor"), "mqtt")
    
    # Broker parameters
    broker_address: str
    broker_port: int
    mqtt_topic: str
    mqtt_qos: int = 0

    latest_reading: str

    # Client
    mqtt_client: mqtt.Client = mqtt.Client()

    # Client Parameters
    client_id: str = mqtt_client._client_id  
    clean_session: bool = mqtt_client._clean_session
    protocol: int = mqtt_client._protocol  # MQTTv31 = 3 MQTTv311 = 4 MQTTv5 = 5
    transport: str = mqtt_client._transport

    # Client Authentication
    client_username: str = ""
    client_password: str = ""

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
        self.protocol = config.attributes.fields['protocol'].string_value if 'protocol' in config.attributes.fields else self.protocol  # MQTTv31 = 3 MQTTv311 = 4 MQTTv5 = 5
        self.transport = config.attributes.fields['transport'].string_value if 'transport' in config.attributes.fields else self.transport  # tcp or websockets
        self.client_username = config.attributes.fields['client_username'].string_value if 'client_username' in config.attributes.fields else self.client_username
        self.client_password = config.attributes.fields['client_password'].string_value if 'client_password' in config.attributes.fields else self.client_password


        if 'mapping_dict' in config.attributes.fields:
            if (config.attributes.fields['mapping_dict'].struct_value):
                self.mapping_dict = config.attributes.fields['mapping_dict'].struct_value
            else:
                self.mapping_dict = config.attributes.fields['mapping_dict'].string_value
        else:
            self.mapping_dict = None

        self.mqtt_client.reinitialise(self.client_id, self.clean_session)

        if self.client_username != "" or self.client_password != "":
            self.mqtt_client.username_pw_set(self.client_username, self.client_password)

        # Set up callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        # Connect to the broker
        self.mqtt_client.connect(self.broker_address, self.broker_port, 60)

        # Start the MQTT loop
        self.mqtt_client.loop_start()
        return

    def on_connect(self, client, userdata, flags, rc):
        LOGGER.info("Connected with result code " + str(rc))
        # Subscribe to the specified topic when connected
        client.subscribe(self.mqtt_topic, qos=self.mqtt_qos)

    def on_message(self, client, userdata, msg):
        # Update the latest reading when a new message is received
        LOGGER.info(f"MQTT Message Received from topic:{msg.topic}")
        self.latest_reading = json.dumps({
            'timestamp': time.time(),
            'topic': msg.topic,
            'payload': json.loads(msg.payload.decode('utf-8')),
            'qos': msg.qos,
            'retain': msg.retain,
            'message_id': msg.mid,
        })

    async def get_readings(self, *, extra: Optional[Mapping[str, Any]] = None, timeout: Optional[float] = None, **kwargs) -> Mapping[str, Any]:
        return_message = self.latest_reading

        if return_message is None:
            return_message = json.dumps({
                'timestamp': 0,
                'topic': "",
                'payload': "",
                'qos': 0,
                'retain': 0,
                'message_id': 0,
            })

        if self.mapping_dict is not None:
            temp_return_message = self.map_json(return_message, self.mapping_dict)
            if(temp_return_message != None):
                return_message = temp_return_message

        return return_message

    @classmethod
    def map_json(cls, json_message, mapping_dict):
        null_value = ''

        result = {}

        for key, value in mapping_dict.items():
            keys = value.split('.')
            current_data = json_message

            for k in keys:
                if isinstance(current_data, dict):
                    current_data = current_data.get(k)
                elif isinstance(current_data, str):
                    try:
                        current_data = json.loads(current_data)
                        current_data = current_data.get(k)
                    except json.JSONDecodeError as e:
                        LOGGER.error(f"Error in mapping.  {e}")
                        current_data = null_value
                else:
                    current_data = null_value 

                if not current_data:
                    current_data = null_value
                    break

            result[key] = current_data  

        return result



async def main():
    # Create config for testing sensor outside Viam app
    from google.protobuf.struct_pb2 import Struct       # needed for ComponentConfig declaration
    from google.protobuf.json_format import ParseDict   # needed for ComponentConfig declaration

    config = ComponentConfig()
    sensor_config =  {
            "broker_address": "test.mosquitto.org",
            "broker_port": 1883,
            "mqtt_topic": "my_test_topic",
            "mapping_dict": {
                "time": "timestamp",
                "temperature": "payload.temperature",
                "humidity": "payload.humidity"
            },
            #"client_username": "<<username>>",
            #"client_password": "<<password>>",
        }

    struct_instance = Struct()
    ParseDict(sensor_config, struct_instance)
    config.attributes.CopyFrom(struct_instance)

    my_mqtt_sensor = mqtt_sensor(name="mqtt-sensor")
    my_mqtt_sensor.validate(config)
    my_mqtt_sensor.reconfigure(config, None)

    while(True):
        await asyncio.sleep(1)
        signal = await my_mqtt_sensor.get_readings()
        print(signal)


if __name__ == '__main__':
    asyncio.run(main())
