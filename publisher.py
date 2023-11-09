import time
import xmlrpc.client
import sys
import json


class Publisher:
    def __init__(self, publisher_id, broker_url):
        self.id = publisher_id
        self.broker_url = broker_url
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)

    def publish(self, topic, message):
        """
        calls the subscribe method on the message broker and packages the information to send in the proper format
        :param topic: channel to publish to
        :param message: content of the message to send
        :return: None
        """
        try:
            message_format = {"topic": topic, "id": self.id, "content": message, "timestamp": time.time()}
            self.broker.publish(message_format)
        except Exception as e:
            print(f"Failed to publish message: {e}")


if __name__ == "__main__":

    num_args = len(sys.argv) - 1
    if num_args == 1:
        # Get the JSON-encoded string from the command line
        json_string = sys.argv[1]

        try:
            # Parse the JSON string into a dictionary
            my_dict = json.loads(json_string)

            # Use the dictionary in your code
            publisher = Publisher(my_dict["id"], my_dict["broker_url"])
            publisher.publish(my_dict["topic"], my_dict["content"])
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Usage: python script.py '<json_string>'")
