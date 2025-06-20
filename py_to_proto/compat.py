"""
Compatibility module for API changes between different versions of protobuf
"""

try:
    # protobuf >= 6
    from google.protobuf.service_reflection import GeneratedServiceType
except ImportError:
    from google.protobuf.service import Service as GeneratedServiceType
