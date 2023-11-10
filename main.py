import string
import time
import random

from broker import MessageBroker
from publisher import Publisher
from subscriber import Subscriber
from concurrent.futures import ThreadPoolExecutor



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

    num_clients = 1000
    broker_url = "http://localhost:8000"  # URL of the message broker server

    topic = "T1"
    topic2 = "T2"
    broker = MessageBroker(broker_url)
    subscriber = Subscriber(1, broker_url, poll_interval=2)
    subscriber.subscribe(topic)
    subscriber.subscribe(topic2)
    subscriber.start_polling()

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for client_id in range(num_clients):
            message = generate_random_message()
            executor.submit(send_message, client_id, broker_url, topic, message)
            time.sleep(1 / num_clients)
            executor.submit(send_message, client_id, broker_url, topic2, message)
            time.sleep(1 / num_clients)

    print(time.time() - start)


