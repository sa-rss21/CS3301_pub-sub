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
        server.register_function(self.get_messages, "get_messages")
        server.serve_forever()

    def subscribe(self, topic, id):
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if not self.message_queue.get_queue(topic):
            self.message_queue.create_queue(topic)
        self.subscribers[topic].append(id)

    def unsubscribe(self, topic, id):
        if topic in self.subscribers and id in self.subscribers[topic]:
            self.subscribers[topic].remove(id)
            if not self.subscribers[topic]:
                del self.subscribers[topic]

    def publish(self, topic, id, message):
        if not self.subscribers.get(topic):
            self.subscribers[topic] = []
        self.message_queue.publish_message(topic, id, message)

    def get_messages(self, topic):
        return list(self.message_queue.get_messages(topic))

    def has_subscribers(self, topic):
        return len(self.subscribers.get(topic, [])) != 0

