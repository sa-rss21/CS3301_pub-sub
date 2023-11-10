import time
import xmlrpc.client
import sys
import json


class Publisher:
    def __init__(self, publisher_id, broker_url, max_retry_attempts=3):
        self.id = publisher_id
        self.broker_url = broker_url
        self.broker = xmlrpc.client.ServerProxy(self.broker_url)
        self.max_retry_attempts = max_retry_attempts

    def publish(self, topic, message):
        """
        calls the subscribe method on the message broker and packages the information to send in the proper format
        :param topic: channel to publish to
        :param message: content of the message to send
        :return: None
        """
        try:
            retry_count = 0
            message_format = {"topic": topic, "id": self.id, "content": message, "send_timestamp": time.time()}
            while retry_count < self.max_retry_attempts:
                try:
                    ret = self.broker.publish(message_format)
                    if ret == 0:
                        break
                except Exception as e:
                    retry_count += 1
                    print(f"Error publishing message, trying again: {e}")
                    time.sleep(0.5)
        except Exception as e:
            print(f"Failed to publish message: {e}")

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

