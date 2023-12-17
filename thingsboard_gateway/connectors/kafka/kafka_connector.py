from json import loads
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    from kafka import KafkaConsumer
except ImportError:
    print("kafka library not found")
    TBUtility.install_package("kafka-python")
    from kafka import KafkaConsumer


class KafkaConnector(Connector, Thread):

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__gateway = gateway
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}
        self.config = config
        self.__log = init_logger(self.__gateway, self.config['name'], self.config.get('logLevel', 'INFO'))
        self.name = self.config.get('name', 'Kafka')
        # Set up Kafka converter ---------------------------------------------------------------------------------------
        self.__kafka_message_converter = DefaultKafkaMessageConverter()
        # Set up Kafka consumer ----------------------------------------------------------------------------------------
        self.__load_kafka_topic_mapper()
        self.__load_kafka_consumer()
        self.__connect_to_devices()
        # Set up lifecycle flags ---------------------------------------------------------------------------------------
        self._connected = False
        self.__running = False
        self.daemon = True

    def __load_kafka_consumer(self):
        self.__kafka_bootstrap_servers = self.config['kafka']['bootstrapServers']
        self.__kafka_consumer = KafkaConsumer(
            bootstrap_servers=self.__kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.config['kafka']['groupId'],
            value_deserializer=lambda x: loads(x.decode('utf-8')))

    def __load_kafka_topic_mapper(self):
        self.__kafka_topic_mapper = KafkaTopicDevicesMapper(self.config['devices'])

    def __connect_to_devices(self):
        topics = self.__kafka_topic_mapper.get_topics()
        self.__kafka_consumer.subscribe(topics)
        self.__log.info("Kafka consumer subscribed to topics: %s", topics)

    def get_config(self):
        return self.config

    def get_type(self):
        return self.__connector_type

    def open(self):
        self.__running = True
        self.start()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def run(self):
        self.__log.info("Kafka connector running!")
        while self.__running:
            self.__log.debug("Kafka connector is running")
            self.__consume()

    def close(self):
        self.__log.info("Kafka connector closed!")
        self.__running = False
        self.__kafka_consumer.unsubscribe()

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __consume(self):
        for message in self.__kafka_consumer:
            self.__log.debug("Message received from topic %s: %s", message.topic, message.value)
            for device in self.__kafka_topic_mapper.get_devices_by_topic(message.topic):
                dict_result = {"deviceName": device["name"], "deviceType": device["type"], "attributes": [], "telemetry": []}
                dict_result["telemetry"].append({"ts": message.timestamp, "values": self.__kafka_message_converter.convert(message.value)})
                self.__gateway.send_to_storage(self.get_name(), dict_result)
                self.__log.debug("Message sent from topic to device: %s", device["name"])
            self.statistics['MessagesReceived'] += 1


class DefaultKafkaMessageConverter:
    '''Default converter for Kafka messages'''

    def convert(self, message) -> dict:
        '''Converts message to thingsboard data'''
        return message


class KafkaTopicDevicesMapper:
    '''Mapper of topics to devices'''
    def __init__(self, config):
        self.__config = config
        self.__mappings = {}
        self.__load_devices()

    def __load_devices(self):
        for device in self.__config:
            if device.get('name') is not None and device.get('topic') is not None and device.get('profile') is not None:
                self.add_device(device.get('name'), device.get('profile'), device.get('topic'))

    def get_devices_by_topic(self, topic) -> list:
        '''Returns list of devices in topic'''
        if topic in self.__mappings:
            return self.__mappings[topic]
        else:
            return None
        
    def get_topic(self, device_name) -> str:
        '''Returns topic for device'''
        for topic in self.__mappings.items():
            for device in self.__mappings[topic]:
                if device.get('device_name') == device_name:
                    return topic
        return None
    
    def get_topics(self) -> list:
        '''Returns list of all topics'''
        return self.__mappings.keys()

    def add_device(self, device_name, device_profile, topic):
        '''Add device to topic'''
        self.__create_topic(topic)
        self.__mappings[topic].append({"name": device_name, "type": device_profile})

    def get_devices(self) -> list:
        '''Returns list of devices in all topics'''
        return self.__mappings

    def __create_topic(self, topic):
        '''Create topic if it doesn't exist'''
        if topic not in self.__mappings:
            self.__mappings[topic] = []
