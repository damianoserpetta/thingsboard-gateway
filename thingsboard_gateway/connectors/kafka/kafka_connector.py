from json import loads
from time import time
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

    TEST_TOPIC = "tb-test-topic"

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__gateway = gateway
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}
        self.config = config
        self.__log = init_logger(self.__gateway, self.config['name'], self.config.get('logLevel', 'INFO'))
        self.name = self.config.get('name', 'Kafka') # TODO: da rivedere.
        self.__kafka_bootstrap_servers = config['kafka']['bootstrapServers']
        self.__kafka_test_topic = self.TEST_TOPIC # TODO: solo per test.
        self.__kafka_consumer = KafkaConsumer(
            bootstrap_servers=self.__kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.__connect_to_devices()
        # Set up lifecycle flags ---------------------------------------------------------------------------------------
        self._connected = False
        self.__running = False
        self.daemon = True

    def __connect_to_devices(self):
        self.__kafka_consumer.subscribe([self.__kafka_test_topic])
        self.__log.info("Kafka consumer subscribed to topic: %s", self.__kafka_test_topic)

    def get_config(self):
        return self.__config

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
            dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
            dict_result["deviceName"] = "test-device"
            dict_result["deviceType"] = "default"
            dict_result["telemetry"].append({"ts": message.timestamp, "values": message.value})
            self.__gateway.send_to_storage(self.get_name(), dict_result)
            self.statistics['MessagesSent'] += 1