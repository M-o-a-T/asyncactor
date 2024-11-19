from __future__ import annotations

import logging
import os
import pytest
import time

from asyncactor.actor import (
    Actor,
    GoodNodeEvent,
    PingEvent,
    TagEvent,
    UntagEvent,
)
from asyncactor.backend import get_transport

import trio

from moat.mqtt.client import open_mqttclient

logging.basicConfig(level=logging.INFO)

N = 20


def _env(s, d, f=lambda x: x):
    es = "MQTT_" + s.upper()
    es = os.environ.get(es, None)
    Config[s] = f(es) if es is not None else d


Config = {}
_env("host", "localhost")
_env("port", 1883, int)
Config = {"uri": f"mqtt://{Config['host']}:{Config['port']}"}


async def read_loop(client, transport):
    while True:
        msg = await client.deliver_message()
        if msg is None:
            return
        await transport.deliver(payload=msg.data)


@pytest.mark.trio
async def test_20_all():
    """
    This test starts multiple servers at the same time and checks that all
    of them get their turn.
    """
    N = 5  # pylint: disable=redefined-outer-name
    tagged = False
    msgs = {}

    async def s1(i, *, task_status):
        nonlocal tagged
        async with open_mqttclient(client_id="act_test_%d" % (i,), config=Config) as C:
            T = get_transport("mqtt")(C, "test_20")
            C._tg.start_soon(read_loop, C, T)
            async with Actor(T, "c_" + str(i), cfg={"nodes": N, "gap": 0.1, "cycle": 1}) as k:
                task_status.started()
                await k.set_value(i * 31)
                c = 0
                t = time.time()
                async for m in k:
                    if i == 1:
                        k.logger.info("*** MSG %d %r", i, m)
                    ot, t = t, time.time()
                    if ot != t:
                        assert tagged <= 1
                    msgs.setdefault(i, []).append(m)
                    if isinstance(m, GoodNodeEvent):
                        pass
                    elif isinstance(m, TagEvent):
                        # assert not tagged  # may collide, so checked above
                        tagged += 1
                        c += 1
                    elif isinstance(m, UntagEvent):
                        assert tagged
                        tagged -= 1
                        if c > 2:
                            break
                assert tagged <= 1
                k.logger.debug("N2 %r", k._values)
                for x in range(1, 6):
                    assert k._values["c_" + str(x)] == x * 31

    async with trio.open_nursery() as tg:
        for i in range(1, 6):
            await tg.start(s1, i)


@pytest.mark.trio
async def test_21_some():
    """
    This test starts multiple servers at the same time and checks that
    some of them are skipped.
    """
    N = 15  # pylint: disable=redefined-outer-name

    msgs = {}

    c = 0
    h = [0] * (N + 1)

    async def s1(i, *, task_status):
        async with open_mqttclient(client_id="act_test_%d" % (i,), config=Config) as C:
            T = get_transport("mqtt")(C, "test_21")
            C._tg.start_soon(read_loop, C, T)
            nonlocal c
            async with Actor(T, "c_" + str(i), cfg={"nodes": 3, "gap": 0.1, "cycle": 1}) as k:
                task_status.started()
                await k.set_value(i * 31)
                async for m in k:
                    msgs.setdefault(i, []).append(m)
                    if i == 1:
                        k.logger.info("*** MSG %d %r", i, m)
                    if isinstance(m, GoodNodeEvent):
                        pass
                    elif isinstance(m, TagEvent):
                        c += 1
                        h[i] += 1
                    elif isinstance(m, (PingEvent, UntagEvent)):
                        if c > 10:
                            assert sum((x > 0) for x in h) < 6
                            return
            for i in range(1, 6):
                assert k._values["c_" + str(i)] == i * 31

    async with trio.open_nursery() as tg:
        for i in range(1, 6):
            await tg.start(s1, i)

        await trio.sleep(10)
    # server end
