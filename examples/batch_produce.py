import asyncio
import random

from aiokafka.producer import AIOKafkaProducer


async def send_many(num):
    topic = "my_topic"
    producer = AIOKafkaProducer()
    await producer.start()

    batch = producer.create_batch()

    i = 0
    while i < num:
        msg = f"Test message {i:d}".encode("utf-8")
        metadata = batch.append(key=None, value=msg, timestamp=None)
        if metadata is None:
            partitions = await producer.partitions_for(topic)
            partition = random.choice(tuple(partitions))
            await producer.send_batch(batch, topic, partition=partition)
            print(f"{batch.record_count():d} messages sent to "
                  f"partition {partition:d}")
            batch = producer.create_batch()
            continue
        i += 1
    partitions = await producer.partitions_for(topic)
    partition = random.choice(tuple(partitions))
    await producer.send_batch(batch, topic, partition=partition)
    print(f"{batch.record_count():d} messages sent to partition {partition:d}")
    await producer.stop()


asyncio.run(send_many(1000))
