from __future__ import annotations

import logging

logging.basicConfig(level=logging.INFO)

import os

import trio._core._run as tcr

if "PYTHONHASHSEED" in os.environ:
    tcr._ALLOW_DETERMINISTIC_SCHEDULING = True
    tcr._r.seed(os.environ["PYTHONHASHSEED"])
