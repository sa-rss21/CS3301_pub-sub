import socket
def connect_to_broker(host, port):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        return client_socket
    except Exception as e:
        print(f"Error connecting to broker: {str(e)}")
        return None