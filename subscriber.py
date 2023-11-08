import threading
import time
import xmlrpc.client
import xmlrpc.server


class Subscriber:

    def __init__(self, id, broker_url):
        self.id = id

        self.broker_url = broker_url
        self.topics = []
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)
        t = threading.Thread(target=self.start_polling)
        t.start()

    def subscribe(self, topic):
        try:
            self.broker.subscribe(topic, self.id)
            self.topics.append(topic)
        except Exception as e:
            print(f"Failed to subscribe: {e}")

    def start_polling(self):
        broker_thread = threading.Thread(target=self.poll_messages())
        broker_thread.start()

    def unsubscribe(self, topic):
        try:
            self.broker.unsubscribe(topic, self.id)
            self.topics.remove(topic)
        except Exception as e:
            print(f"Failed to unsubscribe: {e}")

    def poll_messages(self, polling_interval=5):
        while True:
            try:
                # skip poll if sub is not interested in any topics
                if len(self.topics) == 0:
                    continue
                # Poll for new messages for the subscribed topics
                for topic in self.topics:
                    messages = self.broker.get_messages(topic)
                    if messages:
                        # Process received messages here
                        for message in messages:
                            print(f"Received message: {message}")

                time.sleep(polling_interval)
            except Exception as e:
                print(f"Error while polling for messages: {e}")