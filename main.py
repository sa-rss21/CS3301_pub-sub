import cProfile
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


def send_message(client_id, broker_url, topic, message):
    publisher = Publisher(client_id, broker_url)
    publisher.publish(topic, message)


if __name__ == "__main__":
    start = time.time()
    # Start the broker server in a separate thread

    num_clients = 10
    broker_url = "http://localhost:8000"  # URL of the message broker server

    topic = "T1"
    topic2 = "T2"
    broker = MessageBroker(broker_url)
    subscriber = Subscriber(1, broker_url)
    subscriber.subscribe(topic)

    subscriber2 = Subscriber(2, broker_url)
    subscriber2.subscribe(topic2)


    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients):
            message = generate_random_message()
            executor.submit(send_message, client_id, broker_url, topic, message)
            time.sleep(1 / num_clients)

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients, num_clients*2):
            message = generate_random_message()
            executor.submit(send_message, client_id, broker_url, topic2, message)
            time.sleep(1 / num_clients)

    print(time.time() - start)


