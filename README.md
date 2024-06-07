# Streaming Arrow

<img width="895" alt="streaming_arrow" src="https://github.com/bytewax/streaming-arrow/assets/6073079/227c9577-4467-41c5-9efa-8ba27b1b5e59">

## Background

A complete streaming architecture has never been known for its simplicity. And this is something I have always seen as a major detractor from more widespread adoption. It's also hard to square the problem of half my data coming in batches and half of it being in streams. You will suffer from the lowest common denominator problem when you try to bring products to market. The lowest common denominator problem in data is that you will only be able to provide intelligence at the speed or freshness of your slowest data. This might not be an issue for you, but it could be devastating in many instances. Most new features are powered by data, whether through machine intelligence like AI or as an interactive analytical product. Ordering a ride on Uber, looking at content on TikTok, or checking out while purchasing on amazon.com. All of these experiences are powered by data. More specifically, real-time generated data. When you think of the leading companies/products in many industries today, the speed and freshness of data in these products are key determinants of their success. A key part of that is that the data is used in an algorithm or model to create a better experience or drive more engagement.

There are largely two different patterns that have different requirements when it comes to streaming data.

Event-driven patterns - you care about the single event more than the whole.
Analytical patterns - you care about many events more than a single event.

Analytical tasks are traditionally left to the data warehouse, but if your requirement is to surface insights in near real-time, then you need your lowest common denominator to be near real-time. So, you need to move daily or hourly jobs to minutes or seconds.

An architectural pattern that I've seen bytewax used for is capturing a swath of disparate input sources, normalizing the structure and maybe doing some pre-aggregation, producing the data to a broker like Redpanda, and then consuming it again downstream of the broker. Oftentimes, these are representative of the analytical patterns, and the user cares about both the aggregate of the events and they care about timeliness. As an example, let's say I have many sensors distributed across a supply chain, I might not be able to reason about what is happening from an individual sensor reading, but the sensors in aggregate and over time tell me if the supply chain is functioning correctly. I also don't need to react to every sensor reading immediately, but I need to passively understand what is going on so I can react immediately for safety or monetary reasons.

![architecture](https://github.com/bytewax/streaming-arrow/assets/6073079/4635fd46-d8e7-49c8-bf5a-500249538d72)


We have seen Bytewax adopted for its ability to connect disparate data sources, its ease of use, and the availability of analytical libraries.

The proposed solution to the above problem is great (in my opinion 😄) in that it can actually solve two problems at once! First, we will have our data available aggregated and in a format that can be directly used for analytical purposes, whether that be making a prediction or just engineering features for a model. Second, we have made our ingestion task much easier, as we will find out further on in the article!

### A streaming arrow solution

With the adoption of Kafka as the streaming protocol, Arrow as the in-memory format, and open catalogs for data lakes and lakehouses, we can architect a very friendly and open-source solution.

Let's get streaming!

For this example, we are going to borrow from a Bytewax community user who made a solution for the energy company he works for. First, we will use Bytewax to "gather" events from disparate sources and transform them into micro-batches using the Arrow format. We will then write these Arrow tables to a message broker like Redpanda to provide us with some durability, replayability, and any fan-out workloads downstream. Then downstream we can use Bytewax to run real-time alerting or analysis we need and also create another workload that will load our data to a sink like Clickhouse for additional reporting/querying. 

What's great is that you could sub in literally any Kafka API-compatible streaming platform and substitute any Sink that is arrow-compatible downstream. 

I don't have many sensors at my disposal, so we can use our own computers :smile:.

```python
import pyarrow as pa
from bytewax.connectors.kafka import KafkaSinkMessage, operators as kop
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from datetime import datetime
from time import perf_counter
import psutil


BROKERS = ["localhost:19092"]
TOPIC = ["arrow_tables"]

run_start = perf_counter()

SCHEMA = pa.schema([
    ('device',pa.string()),
    ('ts',pa.timestamp('us')), # microsecond
    ('cpu_used',pa.float32()),
    ('cpu_free',pa.float32()),
    ('memory_used',pa.float32()),
    ('memory_free',pa.float32()),
    ('run_elapsed_ms',pa.int32()),
])

def sample_wide_event():
    return {
        'device':'localhost',
        'ts': datetime.now(),
        'cpu_used': psutil.cpu_percent(),
        'cpu_free': round(1 - (psutil.cpu_percent()/100),2)*100,
        'memory_used': psutil.virtual_memory()[2], 
        'memory_free': round(1 - (psutil.virtual_memory()[2]/100),2)*100, 
        'run_elapsed_ms': int((perf_counter() - run_start)*1000)
    }

def sample_batch_wide_table(n):
    samples = [sample_wide_event() for i in range(n)]
    arrays = []
    for f in SCHEMA:
        array = pa.array([samples[i][f.name] for i in range(n)], f.type)
        arrays.append(array)
    t = pa.Table.from_arrays(arrays, schema=SCHEMA)

    return t

def table_to_compressed_buffer(t: pa.Table) -> pa.Buffer:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(
        sink,
        t.schema,
        options=pa.ipc.IpcWriteOptions(
            compression=pa.Codec(compression="zstd", compression_level=1)
        ),
    ) as writer:
        writer.write_table(t)
    return sink.getvalue()

BATCH_SIZE = 1000
N_BATCHES = 10
table_gen = (sample_batch_wide_table(BATCH_SIZE) for i in range(N_BATCHES))


flow = Dataflow("arrow_producer")

# this is only an example. should use window or collect downstream
tables = op.input("tables", flow, TestingSource(table_gen))

buffers = op.map("string_output", tables, table_to_compressed_buffer)
messages = op.map("map", buffers, lambda x: KafkaSinkMessage(key=None, value=x))
op.inspect("message_stat_strings", messages, lambda x: f"-> {len(x.value)} bytes")
kop.output("kafka_out", messages, brokers=BROKERS, topic=TOPIC)
```

You will notice that we are using Apache Arrow. But wait, why arrow? Isn't this for in-memory tables for columnar data. Well. Yes. But it is also really efficient when we want to avoid copying and serializing the data.

Once our data is in an arrow table, we can use it everywhere and with everything!

