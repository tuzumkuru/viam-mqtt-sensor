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

    def __init__(self, name: str):
        super().__init__(name)
        self.mqtt_client: mqtt.Client = None
        self.latest_reading: str = None

    # Constructor
    @classmethod
    def new(cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]) -> Self:
        sensor = cls(config.name)
        sensor.reconfigure(config, dependencies)
        return sensor

    # Validates JSON Configuration
    @classmethod
    def validate(cls, config):
        # Validate required parameters
        required_params = ['broker_address', 'broker_port', 'mqtt_topic']

        for param in required_params:
            if not param in config.attributes.fields:
                raise Exception(f"{param} must be defined.")
            
        # Validate broker_address and mqtt_topic as strings        
        topic = config.attributes.fields["broker_address"].string_value
        if topic is None or len(topic) == 0:
                    raise ValueError('Invalid broker_address.')
        
        topic = config.attributes.fields["mqtt_topic"].string_value
        if topic is None or len(topic) == 0:
                    raise ValueError('Invalid mqtt_topic.')

        # Validate broker_port as a valid integer
        if 'broker_port' in config.attributes.fields:
            port_value = config.attributes.fields['broker_port'].number_value
            if not isinstance(port_value, (int,float)) or port_value <= 0 or port_value > 65535:
                raise Exception("Invalid broker port number.")

        # Validate optional parameters with specific conditions
        if 'mqtt_qos' in config.attributes.fields:
            qos_value = config.attributes.fields['mqtt_qos'].number_value

            if not isinstance(qos_value, (int, float)) or qos_value not in [0.0, 1.0, 2.0]:
                raise Exception("MQTT QoS must be a valid number and set to 0, 1, or 2")

        if 'protocol' in config.attributes.fields:
            protocol_value = config.attributes.fields['protocol'].number_value
            valid_protocol_values = [3, 4, 5]
            if not protocol_value or protocol_value not in valid_protocol_values:
                raise Exception("Invalid MQTT protocol version.")

        if 'transport' in config.attributes.fields and config.attributes.fields['transport'].string_value not in ['tcp', 'websockets']:
            raise Exception("Invalid transport protocol.")

        if 'clean_session' in config.attributes.fields and not isinstance(config.attributes.fields['clean_session'].bool_value, bool):
            raise Exception("Clean session must be a boolean value.")

        # Validate client_id and clean_session relationship
        if 'clean_session' in config.attributes.fields and not config.attributes.fields['clean_session'].bool_value:
            if not config.attributes.fields['client_id'].string_value:
                raise Exception("client_id must be defined when clean_session is set to false.")

        # # Validate client_username and client_password relationship
        # # Commented out since MQTT protocol does not restrict this. Code is left for reference
        # if 'client_username' in config.attributes.fields or 'client_password' in config.attributes.fields:
        #     if 'client_username' not in config.attributes.fields or 'client_password' not in config.attributes.fields:
        #         raise Exception("Both client_username and client_password must be defined if either is set.")

        # Validate mapping_dict
        if 'mapping_dict' in config.attributes.fields:
            mapping_dict = config.attributes.fields['mapping_dict'].struct_value

            # Check if there is more than 0 key-value pairs
            if len(mapping_dict) == 0:
                raise Exception("Mapping dictionary must contain key-value pairs.")
            
            # Iterate over key-value pairs and check if kv_pair is a string
            for kv_pair in mapping_dict:
                if not isinstance(kv_pair, str) or not isinstance(mapping_dict[kv_pair], str):
                    raise Exception("Each element in the mapping dictionary must be a key-value pair string.")

        # If all validations pass, return
        return


    # Handles attribute reconfiguration
    def reconfigure(self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]):
        if self.mqtt_client is not None:
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()
        
        self.latest_reading = None

        self.broker_address = config.attributes.fields['broker_address'].string_value
        self.broker_port = int(config.attributes.fields['broker_port'].number_value)
        self.mqtt_topic = config.attributes.fields['mqtt_topic'].string_value
        self.mqtt_qos = int(config.attributes.fields['mqtt_qos'].number_value)

        if 'mapping_dict' in config.attributes.fields:
            if (config.attributes.fields['mapping_dict'].struct_value):
                self.mapping_dict = config.attributes.fields['mapping_dict'].struct_value
            else:
                self.mapping_dict = config.attributes.fields['mapping_dict'].string_value
        else:
            self.mapping_dict = None

        client_params = {}

        if 'client_id' in config.attributes.fields:
            client_params["client_id"] = config.attributes.fields['client_id'].string_value

        if 'clean_session' in config.attributes.fields:
            client_params["clean_session"] = bool(config.attributes.fields['clean_session'].bool_value)

        if 'protocol' in config.attributes.fields:
            client_params["protocol"] = config.attributes.fields['protocol'].string_value

        if 'transport' in config.attributes.fields:
            client_params["transport"] = config.attributes.fields['transport'].string_value

        self.mqtt_client = mqtt.Client(**client_params)

        if 'client_username' in config.attributes.fields or 'client_password' in config.attributes.fields:
            if 'client_password' in config.attributes.fields:
                self.mqtt_client.username_pw_set(username=config.attributes.fields['client_username'].string_value, password=config.attributes.fields['client_password'].string_value)
            else:
                self.mqtt_client.username_pw_set(username=config.attributes.fields['client_username'].string_value)

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
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
        except json.JSONDecodeError as e:
            LOGGER.warn(f"Error decoding message payload: {e}")
            payload = {}

        self.latest_reading = json.dumps({
            'timestamp': time.time(),
            'topic': msg.topic,
            'payload': payload,
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

        return json.loads(return_message)

    @classmethod
    def map_json(cls, json_message, mapping_dict) -> str:
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

        return json.dumps(result)


async def main():
    # Create config for testing sensor outside Viam app
    from google.protobuf.struct_pb2 import Struct       # needed for ComponentConfig declaration
    from google.protobuf.json_format import ParseDict   # needed for ComponentConfig declaration

    config = ComponentConfig()
    sensor_config = {
            "broker_address": "test.mosquitto.org",
            "broker_port": 1883,
            "mqtt_topic": "my_test_topic",
            #"mqtt_qos": 0,
            #"protocol" : 5,
            #"transport" : "tcp",
            #"client_id" : "<<client_id>>",
            #"clean_session" : True,
            #"client_username": "<<username>>",
            #"client_password": "<<password>>",
            "mapping_dict": {
                "time": "timestamp",
                "temperature": "payload.temperature",
                "humidity": "payload.humidity"
            }
        }

    struct_instance = Struct()
    ParseDict(sensor_config, struct_instance)
    config.attributes.CopyFrom(struct_instance)

    mqtt_sensor.validate(config)
    
    my_mqtt_sensor = mqtt_sensor.new(config,None)    

    while(True):
        await asyncio.sleep(1)
        signal = await my_mqtt_sensor.get_readings()
        LOGGER.info(signal)


if __name__ == '__main__':
    asyncio.run(main())
