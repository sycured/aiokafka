from argparse import ArgumentParser
import signal

from asyncio import sleep, get_event_loop, set_event_loop_policy, run, \
    CancelledError
from aiokafka import AIOKafkaConsumer
from collections import Counter


class Benchmark:

    def __init__(self, args):
        self._topic = args.topic
        self._bootstrap_servers = args.broker_list
        self._num = args.num
        self._stats_interval = 1
        self._stats = [Counter()]
        self._use_iter = args.use_iter

    async def _stats_report(self, start):
        loop = get_event_loop()
        interval = self._stats_interval
        i = 1
        try:
            while True:
                await sleep(
                    (start + i * interval) - loop.time())
                stats = self._stats[-1]
                self._stats.append(Counter())
                i += 1
                print(
                    f"Consumed {stats['count']} messages "
                    f"in {interval} second(s)."
                )
        except CancelledError:
            stats = sum(self._stats, Counter())
            total_time = loop.time() - start
            print(f"Total consumed {stats['count']} messages "
                  f"in {total_time:.2f} second(s). "
                  f"Avg {stats['count'] // total_time} m/s.")

    async def bench_simple(self):
        topic = self._topic
        loop = get_event_loop()

        consumer = AIOKafkaConsumer(
            topic, group_id="test_group", auto_offset_reset="earliest",
            enable_auto_commit=False,
            bootstrap_servers=self._bootstrap_servers)
        await consumer.start()

        # We start from after producer connect
        reporter_task = loop.create_task(self._stats_report(loop.time()))
        try:
            total_msgs = 0

            while True:
                if not self._use_iter:
                    msg_set = await consumer.getmany(timeout_ms=1000)
                    if not msg_set:
                        break
                    for msgs in msg_set.values():
                        len_msgs = len(msgs)
                        self._stats[-1]['count'] += len_msgs
                        total_msgs += len_msgs
                    if total_msgs > self._num:
                        break
                else:
                    async for msg in consumer:
                        msg
                        self._stats[-1]['count'] += 1
                        total_msgs += 1
                        if total_msgs > self._num:
                            break

        except CancelledError:
            pass
        finally:
            await consumer.stop()
            reporter_task.cancel()
            await reporter_task


def parse_args():
    parser = ArgumentParser(
        description='Benchmark for maximum throughput to broker on consume. '
                    'Topic should already contain messages. Those can be '
                    'populated using produce benchmark.')
    parser.add_argument(
        '-b', '--broker-list', default="localhost:9092",
        help='List of bootstrap servers. Default {default}.')
    parser.add_argument(
        '-n', '--num', type=int, default=500000,
        help='Max number of messagess to consume. Default {default}.')
    parser.add_argument(
        '--topic', default="test",
        help='Topic to consume messages from. Default {default}.')
    parser.add_argument(
        '--uvloop', action='store_true',
        help='Use uvloop instead of asyncio default loop.')
    parser.add_argument(
        '--use-iter', action='store_true',
        help='Use iteration interface rather than getmany()')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.uvloop:
        import uvloop
        set_event_loop_policy(uvloop.EventLoopPolicy())

    run(Benchmark(args).bench_simple())


if __name__ == "__main__":
    main()
