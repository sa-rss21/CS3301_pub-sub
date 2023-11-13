import threading
import time
import xmlrpc.client
import xmlrpc.server
import sys
import json


class Subscriber:

    def __init__(self, subscriber_id, broker_url, poll_interval=2, max_retry_attempts=3):
        self.id = subscriber_id
        self.polling_interval = poll_interval
        self.max_retry_attempts = max_retry_attempts
        self.broker_url = broker_url
        self.topics = []
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)

        self.use_backup_queue = False  # Flag to indicate whether to use backup queue
        self.backup_queue = None  # Backup queue to store messages in case of main queue failure

    def subscribe(self, topic):
        """
        calls the subscribe method on the broker to register this intance's interest in the topic
        :param topic: channel to subscribe to
        :return: None
        """
        retry_count = 0
        while retry_count < self.max_retry_attempts:
            try:
                self.broker.subscribe(topic, self.id)
                self.topics.append(topic)
                break  # Break out of retry loop if successful
            except Exception as e:
                print(f"Failed to subscribe to {topic}: {e}")
                retry_count += 1
                time.sleep(1 / self.polling_interval)

    def start_polling(self):
        """
        starts thread to poll for new messages
        :return: None
        """
        broker_thread = threading.Thread(target=self.poll_messages)
        broker_thread.start()

    def unsubscribe(self, topic):
        """
        calls the unsubscribe method on the broker
        :param topic: channel to unsubscribe from
        :return: None
        """
        try:
            self.broker.unsubscribe(topic, self.id)
            self.topics.remove(topic)
        except Exception as e:
            print(f"Failed to unsubscribe: {e}")

    def poll_messages(self):
        """
        Polls the broker server on an interval to retrieve new messages from the channels the instance
        has been subscribed to.
        :param polling_interval: time interval between polls
        :param max_retry_attempts: If the poll fails, this is the maximum amount of retries that are allowed before
        failing
        :return: None
        """
        while True:
            try:
                # skip poll if sub is not interested in any topics
                if len(self.topics) == 0:
                    time.sleep(self.polling_interval)
                    continue

                for topic in self.topics:
                    retry_count = 0
                    while retry_count < self.max_retry_attempts:  # for connection fault protection
                        try:
                            messages = self.broker.get_messages(topic, self.id)
                            if messages:
                                # Process received messages here
                                for message in messages:
                                    self.handle_message(message, topic)

                            break  # Break out of retry loop if successful
                        except Exception as e:
                            print(f"Error while polling for topic '{topic}': {e}")
                            retry_count += 1
                            try:
                                self.broker = xmlrpc.client.ServerProxy(self.broker_url)
                            except Exception:
                                pass
                            time.sleep(self.polling_interval/2)
                time.sleep(self.polling_interval)

            except Exception as e:
                print(f"Error while polling for messages: {e}")

    def handle_message(self, message, topic):
        print(f"Message from ID-{message['id']} on topic {topic}: {message['content']}")


if __name__ == "__main__":

    num_args = len(sys.argv) - 1
    if num_args == 1:

        id = sys.argv[1]
        try:
            # Parse the JSON string into a dictionary
            my_dict = {"id": id, "broker_url": "http://localhost:8000", "topic": "T1"}
            # Use the dictionary in your code
            subscriber = Subscriber(my_dict["id"], my_dict["broker_url"])
            subscriber.subscribe(my_dict["topic"])
            subscriber.start_polling()
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Usage: python script.py 'id'")