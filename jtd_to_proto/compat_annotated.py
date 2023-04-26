"""
Helper module to allow Annotated to be imported in 3.7 and 3.8
"""

try:
    # Standard
    from typing import Annotated, get_args, get_origin
except ImportError:
    # Third Party
    from typing_extensions import Annotated, get_args, get_origin
