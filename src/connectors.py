from typing import Any, List
from typing_extensions import override

from bytewax.outputs import StatelessSinkPartition, DynamicSink
import adbc_driver_sqlite.dbapi
from pyarrow import Table

class _ADBCSinkPartition(StatelessSinkPartition[Any]):

    def __init__(self):
        self.conn = adbc_driver_sqlite.dbapi.connect()
        self.cursor = self.conn.cursor()

    @override
    def write_batch(self, items: List[Table]) -> None:
        for table in items:
            self.cursor.adbc_ingest("sample", table, mode="append")
    
    @override
    def close(self):
        self.conn.close()

class ADBCSQLiteSink(DynamicSink):
    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> _ADBCSinkPartition:
        return _ADBCSinkPartition()
