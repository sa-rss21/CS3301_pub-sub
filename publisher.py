import xmlrpc.client


class Publisher:
    def __init__(self, id, broker_url):
        self.id = id
        self.broker_url = broker_url
        self.client = xmlrpc.client.ServerProxy(self.broker_url)

    def publish(self, topic, message):
        try:
            self.client.publish(topic, self.id, message)

        except Exception as e:
            print(f"Failed to publish message: {e}")