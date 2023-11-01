import threading
import queue
import socket
import datetime

import time
# Define a class for managing message queues
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
        if channel_name in self.queues:
            self.queues[channel_name].put((channel_name, publisher_id,
                                            time.asctime(), message))
        else:
            print(f"Channel {channel_name} does not exist!")

    def get_messages(self, channel_name):
        # Subscribe to a channel and receive messages
        if channel_name in self.queues:
            while not self.queues[channel_name].empty():
                # return all messages
                message = self.queues[channel_name].get()
                yield message


# Define a class for the message broker
class MessageBroker:
    def __init__(self, host, port):
        self.queue_manager = MessageQueueManager()
        self.subscribers = {}
        self.host = host
        self.port = port

        # Create a socket server to accept connections from publishers and subscribers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

    def start(self):
        # Start a new thread to accept incoming connections
        accept_thread = threading.Thread(target=self.accept_connections)
        accept_thread.start()

    def accept_connections(self):
        while True:
            client_socket, client_address = self.server_socket.accept()
            # Create a new thread to handle the client's requests
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()

    def handle_client(self, client_socket):
        while True:
            try:
                data = client_socket.recv(1024).decode('utf-8')
                if not data:
                    break
                parts = data.split(':', 2)

                if len(parts) == 3:
                    channel_name, client_id, message = parts[0], parts[1], parts[2]
                    self.queue_manager.publish_message(channel_name, client_id, message)
                    self.notify_subscribers(channel_name)
                if parts[0] == 'SUBSCRIBE':
                    # Handle subscription request
                    self.handle_subscription(client_socket, parts[1])
            except Exception as e:
                print(f"Error handling client: {str(e)}")
                break

        client_socket.close()

    def handle_subscription(self, client_socket, channel_name):
        # Subscribe the client to the specified channel
        self.subscribe(channel_name, lambda message: self.send_message_to_subscriber(client_socket, message))
        # print(f"Subscriber subscribed to channel: {channel_name}")

    def send_message_to_subscriber(self, client_socket, message):
        # Send a message to the subscriber
        try:
            client_socket.send(f"MESSAGE:{message}".encode('utf-8'))
        except Exception as e:
            print(f"Error sending message to subscriber: {str(e)}")

    def notify_subscribers(self, channel_name):
        if channel_name in self.subscribers:
            for subscriber_callback in self.subscribers[channel_name]:
                for message in self.queue_manager.get_messages(channel_name):
                    subscriber_callback(message)

    def subscribe(self, channel_name, subscriber_callback):
        if channel_name not in self.subscribers:
            self.subscribers[channel_name] = []
        if channel_name not  in self.queue_manager.queues:
            self.create_channel(channel_name)
        self.subscribers[channel_name].append(subscriber_callback)

    def create_channel(self, channel_name):
        # Create a new channel by creating a message queue
        self.queue_manager.create_queue(channel_name)
        self.subscribers[channel_name] = []

    def delete_channel(self, channel_name):
        # Delete a channel and its associated message queue
        self.queue_manager.delete_queue(channel_name)
        del self.subscribers[channel_name]





