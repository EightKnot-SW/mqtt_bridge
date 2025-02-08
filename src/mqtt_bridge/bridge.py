from abc import ABCMeta
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import inject
import paho.mqtt.client as mqtt
import rospy

from .util import lookup_object, extract_values, populate_instance


def create_bridge(
    factory: Union[str, "Bridge"],
    msg_type: Union[str, Type[rospy.Message]],
    topic_from: str,
    topic_to: str,
    frequency: Optional[float] = None,
    **kwargs,
) -> "Bridge":
    """generate bridge instance using factory callable and arguments. if `factory` or `meg_type` is provided as string,
    this function will convert it to a corresponding object.
    """
    if isinstance(factory, str):
        factory = lookup_object(factory)
    if not issubclass(factory, Bridge):
        raise ValueError("factory should be Bridge subclass")
    if isinstance(msg_type, str):
        msg_type = lookup_object(msg_type)
    if not issubclass(msg_type, rospy.Message):
        raise TypeError("msg_type should be rospy.Message instance or its stringreprensentation")
    return factory(topic_from=topic_from, topic_to=topic_to, msg_type=msg_type, frequency=frequency, **kwargs)


class Bridge(object, metaclass=ABCMeta):
    """Bridge base class"""

    __mqtt_client = inject.attr(mqtt.Client)
    _serialize = inject.attr("serializer")
    _deserialize = inject.attr("deserializer")
    _extract_private_path = inject.attr("mqtt_private_path_extractor")

    def __init__(self):
        self.__subs: List[Tuple[str, int]] = []
        self.__pubs: Dict[str, Tuple[Any, int]] = {}

    def _mqtt_subscribe(self, topic: str, callback: Callable[[mqtt.MQTTMessage], None], qos=0):
        self.__subs.append((topic, qos))
        Bridge.__mqtt_client.message_callback_add(topic, lambda c, u, msg: callback(msg))

    def _mqtt_publish(self, topic: str, payload: Any, qos=0, retain=False):
        if retain:
            if payload:  # This expression ignores int or float cases because they will not be used in our application.
                self.__pubs[topic] = (payload, qos)
            else:
                # Delete retained message.
                del self.__pubs[topic]
        self.__mqtt_client.publish(topic, payload, qos, retain)

    def on_mqtt_connect(self):
        for topic, qos in self.__subs:
            rospy.logdebug(f"Subscribe topic=\"{topic}\", qos={qos}")
            Bridge.__mqtt_client.subscribe(topic, qos)
        for topic, (payload, qos) in self.__pubs.items():
            rospy.logdebug(f"Publish retained topic=\"{topic}\", qos={qos}")
            self.__mqtt_client.publish(topic, payload, qos, True)

    def on_mqtt_disconnect(self):
        pass

    @staticmethod
    def _create_ros_message(mqtt_msg: mqtt.MQTTMessage, msg_type: Type[rospy.Message]) -> rospy.Message:
        """create ROS message from MQTT payload"""
        # Hack to enable both, messagepack and json deserialization.
        if Bridge._serialize.__name__ == "packb":
            msg_dict = Bridge._deserialize(mqtt_msg.payload, raw=False)
        else:
            msg_dict = Bridge._deserialize(mqtt_msg.payload)
        return populate_instance(msg_dict, msg_type())


class RosToMqttBridge(Bridge):
    """Bridge from ROS topic to MQTT

    bridge ROS messages on `topic_from` to MQTT topic `topic_to`. expect `msg_type` ROS message type.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: rospy.Message, frequency: Optional[float] = None):
        super().__init__()
        self._topic_from = topic_from
        self._topic_to = self._extract_private_path(topic_to)
        self._last_published = rospy.get_time()
        self._interval = 0 if frequency is None else 1.0 / frequency
        rospy.Subscriber(topic_from, msg_type, self._callback_ros)

    def _callback_ros(self, msg: rospy.Message):
        now = rospy.get_time()
        if now - self._last_published >= self._interval:
            self._publish(msg)
            self._last_published = now

    def _publish(self, msg: rospy.Message):
        payload = self._serialize(extract_values(msg))
        self._mqtt_publish(self._topic_to, payload)


class MqttToRosBridge(Bridge):
    """Bridge from MQTT to ROS topic

    bridge MQTT messages on `topic_from` to ROS topic `topic_to`. MQTT messages will be converted to `msg_type`.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: Type[rospy.Message], frequency: Optional[float] = None, queue_size: int = 10):
        super().__init__()
        self._topic_from = self._extract_private_path(topic_from)
        self._topic_to = topic_to
        self._msg_type = msg_type
        self._queue_size = queue_size
        self._last_published = rospy.get_time()
        self._interval = None if frequency is None else 1.0 / frequency
        self._mqtt_subscribe(self._topic_from, self._callback_mqtt)
        self._publisher = rospy.Publisher(self._topic_to, self._msg_type, queue_size=self._queue_size)

    def _callback_mqtt(self, mqtt_msg: mqtt.MQTTMessage):
        """callback from MQTT"""
        now = rospy.get_time()

        if self._interval is None or now - self._last_published >= self._interval:
            try:
                ros_msg = Bridge._create_ros_message(mqtt_msg, self._msg_type)
                self._publisher.publish(ros_msg)
                self._last_published = now
            except Exception as e:
                rospy.logerr(e)


__all__ = ["create_bridge", "Bridge", "RosToMqttBridge", "MqttToRosBridge"]
