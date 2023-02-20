"""
Common test helpers
"""

# Standard
import os

# Third Party
from google.protobuf import descriptor_pool, struct_pb2, timestamp_pb2
import pytest

# First Party
import alog

# Global logging config
alog.configure(
    default_level=os.environ.get("LOG_LEVEL", "info"),
    filters=os.environ.get("LOG_FILTERS", ""),
    formatter="json" if os.environ.get("LOG_JSON", "").lower() == "true" else "pretty",
    thread_id=os.environ.get("LOG_THREAD_ID", "").lower() == "true",
)


@pytest.fixture
def temp_dpool():
    """Fixture to isolate the descriptor pool used in each test"""
    dpool = descriptor_pool.DescriptorPool()
    dpool.AddSerializedFile(struct_pb2.DESCRIPTOR.serialized_pb)
    dpool.AddSerializedFile(timestamp_pb2.DESCRIPTOR.serialized_pb)
    yield dpool
