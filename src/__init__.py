"""
This file registers the model with the Python SDK.
"""

from viam.components.sensor import Sensor
from viam.resource.registry import Registry, ResourceCreatorRegistration

from .mqtt_sensor import mqtt_sensor

Registry.register_resource_creator(Sensor.SUBTYPE, mqtt_sensor.MODEL, ResourceCreatorRegistration(mqtt_sensor.new, mqtt_sensor.validate))
