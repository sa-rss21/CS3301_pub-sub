import socket
import threading
from connection import connect_to_broker


class Subscriber:
    def __init__(self, sub_id, broker_host, broker_port):
        self.sub_id = sub_id

        self.subscriptions = []
        self.broker_socket = connect_to_broker(broker_host, broker_port)

    def subscribe(self, channel_name):
        data = f"SUBSCRIBE:{channel_name}"
        self.broker_socket.send(data.encode('utf-8'))
        self.subscriptions.append(channel_name)
        # Start a thread to receive messages from the broker
        receive_thread = threading.Thread(target=self.receive_messages)
        receive_thread.start()

    def receive_messages(self):
        while True:
            try:
                data = self.broker_socket.recv(1024).decode('utf-8')
                if data.startswith("MESSAGE:"):
                    message = data[len("MESSAGE:"):]
                    self.handle_message(message)
            except Exception as e:
                print(f"Error receiving message: {str(e)}")
                break

    def close(self):
        if self.broker_socket:
            self.broker_socket.close()
            self.broker_socket = None


    def handle_message(self, message):
        message =  eval(message)
        print(f"MESSAGE RECEIVED BY SUBSCRIBER {self.sub_id}:\n"
              f"FROM: Client {message[1]} on Channel {message[0]}\n"
              f"TIMESTAMP: {message[2]}\n"
              f"CONTENT: {message[3]}\n")