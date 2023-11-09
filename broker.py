import threading
import time
import xmlrpc.client
import xmlrpc.server
import sys
import json
from queue import PriorityQueue
class MessageQueueManager:
    def __init__(self):
        # Dictionary to store message queues with channel names as keys
        self.queues = {}
        self.queue_limit = 100

    def get_queue(self, topic):
        # Get the message queue for the specified topic
        return self.queues.get(topic)

    def create_topic(self, topic):
        # Create a new message queue for the given channel
        if topic not in self.queues:
            self.queues[topic] = []
            self.queues[topic].append(PriorityQueue())

    def clean_queues(self, topic):
        for queue in self.queues[topic]:
            if queue.empty() and len(self.queues[topic])> 1:
                self.queues[topic].remove(queue)

    def publish_message(self, message):
        # Publish a message to the specified channel
        topic = message["topic"]
        del message["topic"]
        if topic not in self.queues:
            self.create_topic(topic)

        # Find the first non-full queue and put the message there
        for queue in self.queues[topic]:
            if queue.qsize() < self.queue_limit:
                queue.put((message["timestamp"], message))
                return

        new_queue = PriorityQueue()
        new_queue.put((message["timestamp"], message))
        self.queues[topic].append(new_queue)


    def get_messages(self, topic):
        self.display_queue_structure()
        # Subscribe to a channel and receive messages
        if self.get_queue(topic):
            for queue in self.queues[topic]:
                while not queue.empty():
                    # return all messages
                    yield queue.get()[1]

        # clean up empty queues
        self.clean_queues(topic)

    def display_queue_structure(self):
        for topic, queues in self.queues.items():
            print(f"TOPIC: {topic}")
            for i, queue in enumerate(queues):
                print(f"    Queue {i}: {queue.qsize()}/{self.queue_limit} messages")


class MessageBroker:
    """
    Pub-sub Message Broker that uses XML middleware to publish and retreive messages from the MessageQueueManager
    """
    def __init__(self, url):
        self.subscribers = {}
        self.message_queue = MessageQueueManager()
        url_trimmed = url.replace("http://", "").split(":")
        self.host = url_trimmed[0]
        self.port = int(url_trimmed[1])

        broker_thread = threading.Thread(target=self.start_listening)
        broker_thread.start()

    def start_listening(self):
        """
        Starts the XML server to receive requests from clients, either publishers or subscribers
        :return: None
        """
        server = xmlrpc.server.SimpleXMLRPCServer((self.host, self.port), allow_none=True)
        server.register_function(self.subscribe, "subscribe")
        server.register_function(self.unsubscribe, "unsubscribe")
        server.register_function(self.publish, "publish")
        server.register_function(self.get_messages, "get_messages")
        server.serve_forever()

    def subscribe(self, topic, subscriber_id):
        """
        Adds a subscriber to the MessageBroker's subscriber dictionary on the specified channel
        :param topic: channel to subscribe to
        :param subscriber_id: ID of the subscriber
        :return: None
        """
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if not self.message_queue.get_queue(topic):
            self.message_queue.create_topic(topic)
        self.subscribers[topic].append(subscriber_id)

    def unsubscribe(self, topic, subscriber_id):
        """
        Removes the subscriber ID from the specified channel
        :param topic: channel to unsubscribe from
        :param subscriber_id: ID of the subscriber
        :return: None
        """
        if topic in self.subscribers and subscriber_id in self.subscribers[topic]:
            self.subscribers[topic].remove(subscriber_id)
            if not self.subscribers[topic]:
                del self.subscribers[topic]

    def publish(self, message):
        """
        Publishes message to the message queue
        :param message: message-package to unpack and send to message queue manager
        :return: None
        """
        topic = message["topic"]
        if not self.subscribers.get(topic):
            self.subscribers[topic] = []
        self.message_queue.publish_message(message)

    def get_messages(self, topic, subscriber_id):
        """
        Retreives messages from the specified channel
        :param topic: channel to fetch messages from
        :param subscriber_id: ID of the subscriber requesting the message
        :return: None
        """

        # checks that subscriber has access to the data
        if subscriber_id in self.subscribers[topic]:
            return list(self.message_queue.get_messages(topic))
        else:
            print("Subscriber attempting to retreive messages that it is not subscribed to")
            return None

    def has_subscribers(self, topic):
        return len(self.subscribers.get(topic, [])) != 0


if __name__ == "__main__":

    num_args = len(sys.argv) - 1
    if num_args == 1:
        try:
            # Use the dictionary in your code
            broker = MessageBroker(sys.argv[1])
        except Exception as e:
            print("Error starting broker:", e)
    else:
        print("Usage: python script.py 'url'")