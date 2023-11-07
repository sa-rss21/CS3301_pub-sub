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
    profiler = cProfile.Profile()
    profiler.enable()
    start = time.time()
    # Start the broker server in a separate thread

    num_clients = 10
    broker_url = "http://localhost:8000"  # URL of the message broker server

    topic = "topic1"

    broker = MessageBroker(broker_url)

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients):
            message = generate_random_message()
            executor.submit(send_message, client_id, broker_url, topic, message)
            time.sleep(1 / num_clients)

    subscriber = Subscriber(1, broker_url, "http://localhost:9000")
    subscriber.subscribe(topic)


    send_message(1, broker_url, topic, generate_random_message())
    print(time.time() - start)
    profiler.disable()
    profiler.print_stats(sort='cumulative')  # Print profiling statistics

