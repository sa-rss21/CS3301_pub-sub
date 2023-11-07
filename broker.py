import queue
import threading
import time
import xmlrpc.client
import xmlrpc.server
import cProfile


class MessageQueueManager:
    def __init__(self):
        # Dictionary to store message queues with channel names as keys
        self.queues = {}

    def get_queue(self, topic):
        # Get the message queue for the specified topic
        return self.queues.get(topic)

    def create_queue(self, topic):
        # Create a new message queue for the given channel
        if topic not in self.queues:
            self.queues[topic] = queue.Queue()

    def delete_queue(self, topic):
        # Delete a message queue for the given channel
        if topic in self.queues:
            del self.queues[topic]

    def publish_message(self, topic, publisher_id, message):
        # Publish a message to the specified channel
        if topic not in self.queues:
            self.create_queue(topic)

        self.queues[topic].put((topic, publisher_id, time.asctime(), message))

    def get_messages(self, topic):
        # Subscribe to a channel and receive messages

        if self.get_queue(topic):
            while not self.queues[topic].empty():
                # return all messages
                yield self.queues[topic].get()


class MessageBroker:
    def __init__(self, url):
        self.subscribers = {}
        self.message_queue = MessageQueueManager()
        url_trimmed = url.replace("http://", "").split(":")
        self.host = url_trimmed[0]
        self.port = int(url_trimmed[1])

        broker_thread = threading.Thread(target=self.start_listening)
        broker_thread.start()

    def start_listening(self):
        server = xmlrpc.server.SimpleXMLRPCServer((self.host, self.port), allow_none=True)
        server.register_function(self.subscribe, "subscribe")
        server.register_function(self.publish, "publish")
        server.serve_forever()

    def subscribe(self, topic, callback):
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if not self.message_queue.get_queue(topic):
            self.message_queue.create_queue(topic)
        sub = xmlrpc.client.ServerProxy(callback)
        self.subscribers[topic].append(sub)

    def unsubscribe(self, topic, callback):
        if topic in self.subscribers and callback in self.subscribers[topic]:
            self.subscribers[topic].remove(callback)
            if not self.subscribers[topic]:
                del self.subscribers[topic]

    def notify_subscribers(self, topic):
        if self.has_subscribers(topic):
            messages = list(self.message_queue.get_messages(topic))
            self.notify_subscriber(topic, messages)

    def notify_subscriber(self, topic, messages):
        if not self.has_subscribers(topic):
            return
        for subscriber in self.subscribers[topic]:
            try:
                ret = subscriber.notify(messages)
            except Exception as e:
                print(f"Failed to notify subscriber for topic {topic}: {e}")

    def publish(self, topic, id, message):
        if not self.subscribers.get(topic):
            self.subscribers[topic] = []
        self.message_queue.publish_message(topic, id, message)

        # Schedule the notification after a delay (e.g., 5 seconds)
        self.notify_subscribers(topic)

    def has_subscribers(self, topic):
        return len(self.subscribers.get(topic, [])) != 0

