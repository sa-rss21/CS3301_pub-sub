import xmlrpc.client
import xmlrpc.server
import threading

class Subscriber:

    def __init__(self, id, broker_url, callback_url):
        self.id = id
        self.callback_url = callback_url
        url_trimmed = self.callback_url.replace("http://", "").split(":")
        self.host = url_trimmed[0]
        self.port = int(url_trimmed[1])
        self.client_id = None
        self.broker_url = broker_url
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)

        # Start the subscriber in a separate thread
        broker_thread = threading.Thread(target=self.start_listening)
        broker_thread.daemon = True  # So that it terminates when the main program exits
        broker_thread.start()

    def subscribe(self, topic):
        try:
            if self.client_id is None:
                self.client_id = self.broker.subscribe(topic, self.callback_url)
        except Exception as e:
            print(f"Failed to subscribe: {e}")
        return self.id

    def start_listening(self):
        server = xmlrpc.server.SimpleXMLRPCServer((self.host, self.port), allow_none=True)
        server.register_instance(self)
        server.serve_forever()

    def notify(self, messages):
        for message in messages:
            print(f"Received message by subscriber {self.id}: {message}")

    def unsubscribe(self, topic):
        try:
            if self.client_id is not None:
                self.broker.unsubscribe(topic, self.client_id)
                self.client_id = None
        except Exception as e:
            print(f"Failed to unsubscribe: {e}")
