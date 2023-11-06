import queue
import time
from enum import Enum
import threading
from spyne import Application, ServiceBase, Iterable, Unicode, rpc
import asyncio
from spyne import Application, rpc, ServiceBase, Unicode, Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.model.complex import ComplexModel
from spyne.model.primitive import String


class Message(Enum):
    TYPE = "TYPE"
    SENDER = "SENDER"
    CONTENT = "CONTENT"
    TIMESTAMP = "TIME"
    CHANNEL = "CH"


class ReplyToHeader(ComplexModel):
    message_id = String


class MessageIDHeader(ComplexModel):
    correlation_id = String


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


class MessageBroker(ServiceBase):

    subscribers = {}
    queue_manager = MessageQueueManager()

    @rpc(Unicode,_returns=Iterable(Unicode), _is_async=True)
    def publish_message(ctx, message):
        try:
            print(message)
            message_split = message.split(":")
            print(MessageBroker.queue_manager.queues)

            if message_split[0] == "PUBLISH":
                MessageBroker.queue_manager.publish_message(message_split[1], message_split[2], message_split[3])

        except Exception as e:
            print(f"Error handling message: {str(e)}")
        yield message

    def create_channel(self, channel_name):
        # Create a new channel by creating a message queue
        self.queue_manager.create_queue(channel_name)
        self.subscribers[channel_name] = []

    def delete_channel(self, channel_name):
        # Delete a channel and its associated message queue
        self.queue_manager.delete_queue(channel_name)
        del self.subscribers[channel_name]


