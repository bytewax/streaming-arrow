from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
import pyarrow as pa

from connectors import ADBCSQLiteSink

BROKERS = ["localhost:19092"]
TOPICS = ["arrow_tables"]

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=BROKERS, topics=TOPICS)
op.inspect("inspect-errors", kinp.errs)
op.inspect("inspect-oks", kinp.oks)

tables = op.map("tables", kinp.oks, lambda msg: pa.ipc.open_file(msg.value).read_all())
op.inspect("message_stat_strings", tables, lambda t: f"{t.shape} {t['ts'][0]}")
op.output("output_sqlite", kinp.oks, ADBCSQLiteSink)