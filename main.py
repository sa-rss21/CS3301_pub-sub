import string
import time
import xmlrpc.server
import random

from broker import MessageBroker
from publisher import Publisher
from subscriber import Subscriber
from concurrent.futures import ThreadPoolExecutor
import threading


def generate_random_message():
    # Generate a random message
    characters = string.ascii_letters + string.digits  # You can modify this pool as per your requirements
    return ''.join(random.choice(characters) for _ in range(4))


def broker_server():
    try:
        server = xmlrpc.server.SimpleXMLRPCServer(("localhost", 8000), allow_none=True)
        server.register_instance(MessageBroker())
        server.serve_forever()
    except Exception as e:
        print(f"Error in broker_server: {e}")


def send_message(client_id, broker_url, topic, message):
    publisher = Publisher(client_id, broker_url)
    publisher.publish(topic, message)


def start_subscribers():
    subscriber = Subscriber(1, broker_url, "http://localhost:9000")
    subscriber.subscribe(topic)


if __name__ == "__main__":
    # Start the broker server in a separate thread
    broker_thread = threading.Thread(target=broker_server)
    broker_thread.daemon = True  # So that it terminates when the main program exits
    broker_thread.start()

    num_clients = 10
    broker_url = "http://localhost:8000/"  # URL of the message broker server
    topic = "topic1"


    sub_thread = threading.Thread(target=start_subscribers)
    sub_thread.daemon = True  # So that it terminates when the main program exits
    sub_thread.start()

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients):
            message = generate_random_message()
            executor.submit(send_message, client_id, broker_url, topic, message)
            time.sleep(1 / num_clients)



