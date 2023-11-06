from zeep import Client
from zeep.cache import SqliteCache
from zeep.transports import Transport


class Publisher:
    def __init__(self, client_id, service_url):

        self.client_id = client_id
        transport = Transport(SqliteCache())
        self.client = Client(service_url, transport=transport)
    def send_message(self, message: str):
        try:

            result = self.client.service.publish_message(message)
            return result
        except Exception as e:
            # Handle any exceptions that may occur during the SOAP request
            print(f"Error sending message: {str(e)}")
            return None
