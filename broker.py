import threading
import time
import xmlrpc.client
import xmlrpc.server
from queue import PriorityQueue


class MessageQueueManager:
    def __init__(self):
        # Dictionary to store message queues with channel names as keys
        self.queues = {}
        self.queue_limit = 100

    def get_queue(self, topic):
        """
        Grab the requested topic list of queues
        :param topic: topic to retreive
        :return: a list of PriorityQueues or None
        """
        # Get the message queue for the specified topic
        if self.queues.get(topic):
            return self.queues.get(topic)
        else:
            return None

    def create_topic(self, topic):
        """
        Create a new topic of interest in the queue manager
        :param topic: topic to create
        :return: None
        """
        # Create a new message queue for the given channel
        if topic not in self.queues:
            self.queues[topic] = []
            self.queues[topic].append(PriorityQueue())

    def clean_queues(self, topic):
        """
        Iterates through a topic and removes any empty queues
        :param topic: topic to search
        :return: None
        """
        for queue in self.queues[topic]:
            if queue.empty() and len(self.queues[topic])> 1:
                self.queues[topic].remove(queue)

    def publish_message(self, message):
        """
        Takes the given message dictionary and publishes it in the correct queue
        :param message: dictionary with message contents
        :return: None
        """
        # Publish a message to the specified channel
        topic = message["topic"]
        del message["topic"]
        if topic not in self.queues:
            self.create_topic(topic)

        # Find the first non-full queue and put the message there
        for queue in self.queues[topic]:
            if queue.qsize() < self.queue_limit:
                queue.put((message["send_timestamp"], message))
                return 0

        if len(self.queues[topic]) >= self.queue_limit:
            print("Memory Overflow! Message cannot be placed in channel")
            return -1

        # create new queue if there is room
        new_queue = PriorityQueue()
        new_queue.put((message["send_timestamp"], message))
        self.queues[topic].append(new_queue)
        return 0

    def get_messages(self, topic, debug):
        """
        Retrieves all new messages in a given topic
        :param topic: topic to search
        :param debug: print queue structure if true
        :return: list of messages
        """
        if debug:
            self.display_queue_structure()
        # Subscribe to a channel and receive messages
        if self.get_queue(topic):
            for queue in self.queues[topic]:
                while not queue.empty():
                    # return all messages
                    message = queue.get()[1]
                    yield message

        # clean up empty queues
        self.clean_queues(topic)
        return 0

    def display_queue_structure(self):
        """
        displays the current layout of the queue structure
        :return: None
        """
        for topic, queues in self.queues.items():
            print(f"TOPIC: {topic}")
            for i, queue in enumerate(queues):
                print(f"    Queue {i}: {queue.qsize()}/{self.queue_limit} messages")


class MessageBroker:
    """
    Pub-sub Message Broker that uses XML middleware to publish and retrieve messages from the MessageQueueManager
    """
    def __init__(self, url, debug=False):
        self.subscribers = {}
        self.message_queue = MessageQueueManager()
        self.backup_queue = MessageQueueManager()
        url_trimmed = url.replace("http://", "").split(":")
        self.host = url_trimmed[0]
        self.port = int(url_trimmed[1])
        self.debug = debug

        self.sync_threshold = 100
        self.message_count = 0
        self.server = None
        broker_thread = threading.Thread(target=self.start_listening)
        broker_thread.start()

    def start_listening(self):
        """
        Starts the XML server to receive requests from clients, either publishers or subscribers
        :return: None
        """
        self.server = xmlrpc.server.SimpleXMLRPCServer((self.host, self.port), allow_none=True)
        self.server.register_function(self.subscribe, "subscribe")
        self.server.register_function(self.unsubscribe, "unsubscribe")
        self.server.register_function(self.publish, "publish")
        self.server.register_function(self.get_messages, "get_messages")
        self.server.serve_forever()

    def sync_queues(self):
        if self.message_count >= self.sync_threshold and self.message_queue.queues is not None:
            self.backup_queue.queues = self.message_queue.queues
            self.message_count = 0

    def recover_queue(self):
        if self.message_queue.queues is None:
            self.message_queue.queues = self.backup_queue.queues

    def subscribe(self, topic, subscriber_id):
        """
        Adds a subscriber to the MessageBroker's subscriber dictionary on the specified channel
        :param topic: channel to subscribe to
        :param subscriber_id: ID of the subscriber
        :return: None
        """
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        if not self.message_queue.get_queue(topic):
            self.message_queue.create_topic(topic)
        self.subscribers[topic].append(subscriber_id)

    def unsubscribe(self, topic, subscriber_id):
        """
        Removes the subscriber ID from the specified channel
        :param topic: channel to unsubscribe from
        :param subscriber_id: ID of the subscriber
        :return: None
        """
        if topic in self.subscribers and subscriber_id in self.subscribers[topic]:
            self.subscribers[topic].remove(subscriber_id)
            if not self.subscribers[topic]:
                del self.subscribers[topic]

    def kill_queue(self):
        self.message_queue.queues = None

    def publish(self, message):
        """
        Publishes message to the message queue
        :param message: message-package to unpack and send to message queue manager
        :return: None
        """
        ret = -1
        retry_count = 0
        while retry_count < 3:
            try:
                topic = message["topic"]
                if not self.subscribers.get(topic):
                    self.subscribers[topic] = []
                ret = self.message_queue.publish_message(message)
                self.message_count += 1
                self.sync_queues()
                break
            except Exception as e:
                print(f"Queue crashed, retreiving backup and retrying: {e}")
                self.recover_queue()
                retry_count += 1

        return ret

    def get_messages(self, topic, subscriber_id):
        """
        Retreives messages from the specified channel
        :param topic: channel to fetch messages from
        :param subscriber_id: ID of the subscriber requesting the message
        :return: None
        """
        messages = None
        # checks that subscriber has access to the data
        if subscriber_id in self.subscribers[topic]:

            try:
                messages = list(self.message_queue.get_messages(topic, self.debug))
            except Exception as e:
                print(f"Error retrieving messages, queue crashed, getting backup: {e}")
                self.recover_queue()
                messages = list(self.message_queue.get_messages(topic, self.debug))
        else:
            print("Subscriber attempting to retreive messages that it is not subscribed to")

        return sorted(messages, key=lambda x: x["send_timestamp"])

    def connection_interruption(self):
        self.server.shutdown()
        time.sleep(5)
        broker_thread = threading.Thread(target=self.start_listening)
        broker_thread.start()

    def has_subscribers(self, topic):
        return len(self.subscribers.get(topic, [])) != 0


if __name__ == "__main__":
    try:
        # Use the dictionary in your code
        broker = MessageBroker("http://localhost:8000")
    except Exception as e:
        print("Error starting broker:", e)
