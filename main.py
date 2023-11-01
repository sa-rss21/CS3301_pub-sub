import time

from publisher import Publisher
from subscriber import Subscriber
from broker import MessageBroker
import time
if __name__ == "__main__":
    broker_host = "localhost"  # Update this to the actual host of your broker
    broker_port = 12345  # Update this to the desired port for your broker

    broker = MessageBroker(broker_host, broker_port)
    broker.start()

    client1 = Publisher(1, broker_host, broker_port)
    client2 = Publisher(2, broker_host, broker_port)
    sub1 = Subscriber(1, broker_host, broker_port)
    sub2 = Subscriber(2, broker_host, broker_port)

    sub1.subscribe("CH1")
    sub2.subscribe("CH3")
    time.sleep(1)
    # send a message from publisher 0 to all channels
    client1.send_message('CH1', "Hey from client 0 on CH 1")
    client2.send_message('CH3', "Hey from client 0 on CH 3")

    #issues, only the first subscriber is getting the messages

