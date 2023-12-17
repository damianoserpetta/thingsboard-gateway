'''Mapper of topics to devices'''


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