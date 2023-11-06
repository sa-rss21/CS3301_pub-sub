import queue
import time
import xmlrpc.server
import xmlrpc.client


class MessageQueueManager:
    def __init__(self):
        # Dictionary to store message queues with channel names as keys
        self.queues = {}

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
        messages = []
        if topic in self.queues:
            while not self.queues[topic].empty():
                # return all messages
                message = self.queues[topic].get()
                messages.append(message)
        return messages


class MessageBroker:
    def __init__(self):
        self.subscribers = {}
        self.message_queue = MessageQueueManager()

    def subscribe(self, topic, callback):
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if topic not in self.message_queue.queues:
            self.message_queue.create_queue(topic)
        subscriber = xmlrpc.client.ServerProxy(callback)
        self.subscribers[topic].append(subscriber)

    def unsubscribe(self, topic, callback):
        if topic in self.subscribers and callback in self.subscribers[topic]:
            self.subscribers[topic].remove(callback)
            if len(self.subscribers[topic]) == 0:
                del self.subscribers[topic]

    def notify_subscribers(self, topic):
        if topic in self.subscribers:
            messages = self.message_queue.get_messages(topic)
            for subscriber in self.subscribers[topic]:
                try:
                    subscriber.notify(messages)
                except Exception as e:
                    print(f"Failed to notify subscriber for topic {topic}: {e}")

    def publish(self, topic, id, message):
        # create a new subscriber list for topic
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.message_queue.publish_message(topic, id, message)
        if self.has_subscribers(topic):
            self.notify_subscribers(topic)

    def has_subscribers(self, topic):
        return len(self.subscribers[topic]) != 0

