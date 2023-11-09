from subscriber import Subscriber
from publisher import Publisher
from broker import MessageBroker

async def main():
    broker = MessageBroker()
    broker.subscribe("topic1", "http://localhost:8000/callback")
    await broker.publish("topic1", "123", "Hello, World!")

if __name__ == "__main__":
    asyncio.run(main())
