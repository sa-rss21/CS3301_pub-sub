from zeep import Client, Transport
from zeep.cache import SqliteCache


class Subscriber:
    def __init__(self, client_id, service_url):
        self.client_id = client_id
        transport = Transport(cache=SqliteCache())
        self.client = Client(service_url, transport=transport)

    def subscribe(self, channel: str):
        try:
            result = self.client.service.subscribe_to_channel(channel)
            return result
        except Exception as e:
            # Handle any exceptions that may occur during the SOAP request
            print(f"Error sending message: {str(e)}")
            return None

