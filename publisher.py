import string
import threading
import time
import xmlrpc.client
import sys
import random


def generate_random_message():
    # Generate a random message
    characters = string.ascii_letters + string.digits  # You can modify this pool as per your requirements
    return ''.join(random.choice(characters) for _ in range(4))


class Publisher:
    def __init__(self, publisher_id, broker_url, max_retry_attempts=3):
        self.id = publisher_id
        self.broker_url = broker_url
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)
        self.max_retry_attempts = max_retry_attempts
        self.backlog = []
        self.backlog_polling = False

    def publish(self, topic, message):
        """
        calls the subscribe method on the message broker and packages the information to send in the proper format
        :param topic: channel to publish to
        :param message: content of the message to send
        :return: None
        """
        try:
            retry_count = 0
            # packages up message to send over XML
            message_format = {"topic": topic, "id": self.id, "content": message, "send_timestamp": time.time()}

            while retry_count < self.max_retry_attempts: # for connection fault protection
                try:
                    ret = self.broker.publish(message_format)

                    # if there is an overflow
                    if ret == -1:
                        self.backlog.append(message_format)
                        self.start_backlog()

                    break
                except Exception:
                    retry_count += 1
                    print(f"Error publishing message, trying again")
                    # attempt to reconnect
                    try:
                        self.broker = xmlrpc.client.ServerProxy(self.broker_url)
                    except Exception:
                        pass
                    time.sleep(1)
        except Exception as e:
            print(f"Failed to publish message: {e}")
            # put in backlog if message failed
            self.backlog.append(message_format)
            self.start_backlog()

    def disconnect(self):
        """
        Disconnects the publisher from the broker by closing the XML-RPC connection
        :return: None
        """
        try:
            # Close the XML-RPC connection
            self.broker = None
        except Exception as e:
            print(f"Failed to disconnect: {e}")

    def publish_backlog(self):
        """
        Publishes every message in the backlog if there are any
        :return: None
        """
        while len(self.backlog) != 0:
            time.sleep(3)

            if len(self.backlog) > 0:
                for message in self.backlog:
                    ret = self.broker.publish(message)
                    if ret == 0:
                        self.backlog.remove(message)

    def start_backlog(self):
        """
        starts thread to poll for new messages
        :return: None
        """
        if self.backlog_polling:
            return
        self.backlog_polling = True
        broker_thread = threading.Thread(target=self.publish_backlog())
        broker_thread.start()


if __name__ == "__main__":

    num_args = len(sys.argv) - 1
    if num_args == 1:

        id = sys.argv[1]
        try:
            my_dict = {"id": id, "broker_url": "http://localhost:8000", "topic": "T1",
                       "content": generate_random_message()}
            # Use the dictionary in your code
            publisher = Publisher(my_dict["id"], my_dict["broker_url"])
            publisher.publish(my_dict["topic"], my_dict["content"])
        except Exception as e:
            print("Error: e")
    else:
        print("Usage: python script.py 'id'")

