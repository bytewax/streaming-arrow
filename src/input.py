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
