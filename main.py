import time
from concurrent.futures import ThreadPoolExecutor
import random
import string
from publisher import Publisher
from broker import MessageBroker
from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server


def start_server():
    app = Application([MessageBroker], tns='http://example.com/pub', in_protocol=Soap11(validator='lxml'),
                      out_protocol=Soap11())
    server = make_server('127.0.0.1', 8000, WsgiApplication(app))
    server.serve_forever()


def generate_random_message(id):
    # Generate a random message
    message = ''.join(random.choice(string.ascii_letters) for _ in range(10))
    return f"PUBLISH:CH{id}:{id}:{message}"


def run_client(id, url):
    client = Publisher(id, url)

    message = generate_random_message(id)
    client.send_message(message)



if __name__ == '__main__':
    url = 'http://localhost:8000?wsdl'
    messages = 1000

    with ThreadPoolExecutor(max_workers=messages) as executor:
        # Start the server concurrently
        executor.submit(start_server)

        # Simulate 1000 clients independently publishing one message each
        for i in range(messages):
            executor.submit(run_client, i, url)
            time.sleep(1/messages)
    # The main thread doesn't wait for clients to complete, so it can exit without blocking them
