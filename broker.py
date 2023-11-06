import queue
import time
from enum import Enum

from spyne.model.complex import ComplexModel
from spyne.model.primitive import String
import xmlrpc.server



class MessageQueueManager:
    def __init__(self):
        # Dictionary to store message queues with channel names as keys
        self.queues = {}

    def create_queue(self, channel_name):
        # Create a new message queue for the given channel
        if channel_name not in self.queues:
            self.queues[channel_name] = queue.Queue()

    def delete_queue(self, channel_name):
        # Delete a message queue for the given channel
        if channel_name in self.queues:
            del self.queues[channel_name]

    def publish_message(self, channel_name, publisher_id, message):
        # Publish a message to the specified channel
        if channel_name not in self.queues:
            self.create_queue(channel_name)

        self.queues[channel_name].put((channel_name, publisher_id,
                                            time.asctime(), message))

    def get_messages(self, channel_name):
        # Subscribe to a channel and receive messages
        if channel_name in self.queues:
            while not self.queues[channel_name].empty():
                # return all messages
                message = self.queues[channel_name].get()
                yield message


class MessageBroker:
    def __init__(self):
        self.subscribers = {}
        self.message_queue = MessageQueueManager()
    def subscribe(self, topic, callback_url):
        # Implement subscription logic
        pass

    def publish(self, topic, id, message):
        # Implement publishing logic
        print(f"Message by Client: {id} published to topic {topic}")
        self.message_queue.publish_message(topic, id, message)



