import sys
sys.path.append("../../")

import unittest
from os import path
from time import sleep
from unittest.mock import Mock

from simplejson import load
import json

from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.connectors.kafka.kafka_connector import KafkaConnector

from kafka import KafkaProducer

class KafkaConnectorTestsBase(unittest.TestCase):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                            "data" + path.sep + "kafka" + path.sep)
    
    def setUp(self) -> None:
        self.gateway = Mock(spec=TBGatewayService)
        self.gateway.get_devices.return_value = []
        self.connector = None
        self.config = None

    def tearDown(self):
        self.connector.close()

    def _create_connector(self, config_file_name):
        with open(self.CONFIG_PATH + config_file_name, 'r', encoding="UTF-8") as file:
            self.config = load(file)
            self.connector = KafkaConnector(self.gateway, self.config, "kafka")
            self.connector.open()
            sleep(2)

class KafkaConnectorConsumeTest(KafkaConnectorTestsBase):
    __kafka_producer = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__kafka_producer = KafkaProducer(bootstrap_servers='192.168.1.131:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_produce_consume(self):
        self._create_connector('kafka_test_config.json')

        self.__kafka_producer.send('tb-test-topic', {"temperature": 25.0})
        while True:
            sleep(.5)

if __name__ == '__main__':
    unittest.main()