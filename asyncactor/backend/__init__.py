"""
Select a transport backend for asyncactor
"""

from __future__ import annotations


def get_transport(name):
    "import the named transport class"
    from importlib import import_module

    if "." not in name:
        name = "asyncactor.backend." + name
    return import_module(name).Transport
