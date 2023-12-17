'''Default converter for Kafka messages'''


class DefaultKafkaMessageConverter:
    '''Default converter for Kafka messages'''

    def convert(self, message) -> dict:
        '''Converts message to thingsboard data'''
        return message
