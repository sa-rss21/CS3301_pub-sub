import time
import xmlrpc.server
from broker import MessageBroker
from publisher import Publisher
from concurrent.futures import ThreadPoolExecutor
import threading

def broker_server():
    server = xmlrpc.server.SimpleXMLRPCServer(("localhost", 8000), allow_none=True)
    server.register_instance(MessageBroker())
    server.serve_forever()

def send_message(client_id, broker_url, topic, message):
    publisher = Publisher(client_id, broker_url)
    publisher.publish(topic, message)

if __name__ == "__main__":
    # Start the broker server in a separate thread
    broker_thread = threading.Thread(target=broker_server)
    broker_thread.daemon = True  # So that it terminates when the main program exits
    broker_thread.start()

    num_clients = 1000
    broker_url = "http://localhost:8000/"  # URL of the message broker server
    topic = "topic1"
    message = "Hello, World!"

    # Create clients and have them send one message each concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients):
            executor.submit(send_message, client_id, broker_url, topic, message)

