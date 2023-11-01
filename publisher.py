from connection import connect_to_broker


class Publisher:
    def __init__(self, client_id, broker_host, broker_port):
        self.client_id = client_id
        self.broker_socket = connect_to_broker(broker_host, broker_port)

    def send_message(self, channel_name, message):
        if self.broker_socket:
            data = f"{channel_name}:{self.client_id}:{message}"
            self.broker_socket.send(data.encode('utf-8'))


