"""Exercise LAN framing and topology recovery without exposed listening ports."""

import asyncio
import socket
from contextlib import asynccontextmanager

import pytest

from custom_components.yeelight_pro.lan_contract import (
    decode_lan_frames,
    encode_lan_frame,
)
from custom_components.yeelight_pro.lan_runtime import LanGatewayRuntime


@asynccontextmanager
async def connected_runtime(peer, receive, endpoint="gateway"):
    """Use real stream sockets without interference from local port scanners."""
    peers = []

    async def open_connection(host, port):
        client, server = socket.socketpair()
        reader, writer = await asyncio.open_connection(sock=client)
        peer_reader, peer_writer = await asyncio.open_connection(sock=server)
        peers.append(asyncio.create_task(peer(peer_reader, peer_writer)))
        return reader, writer

    runtime = LanGatewayRuntime(
        "unused", endpoint_kind=endpoint, open_connection=open_connection,
    )
    try:
        await runtime.async_start(receive)
        yield runtime
    finally:
        await runtime.async_stop()
        await asyncio.gather(*peers)


@pytest.mark.asyncio
@pytest.mark.parametrize("split", [1, 2])
async def test_utf8_character_split_across_reads(split):
    payload = {"method": "gateway_post.topology", "name": "主卫浴室灯带"}
    wire = encode_lan_frame(payload)
    cut = wire.index("主".encode()) + split
    received = asyncio.get_running_loop().create_future()

    async def peer(reader, writer):
        try:
            writer.write(wire[:cut])
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.write(wire[cut:])
            await writer.drain()
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    async def receive(message):
        if not received.done():
            received.set_result(message)

    async with connected_runtime(peer, receive):
        assert await asyncio.wait_for(received, 1) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint,method", [
    ("gateway", "gateway_get.topology"),
    ("wifi_panel", "device_get.topology"),
])
async def test_reconnect_requests_and_receives_topology(endpoint, method):
    recovered = asyncio.get_running_loop().create_future()
    requests = []
    payload = {"method": "gateway_post.topology", "name": "主卫"}

    async def peer(reader, writer):
        try:
            request = await reader.readline()
            if not request:
                return
            requests.append(decode_lan_frames(request)[0])
            if len(requests) == 1:
                return  # Drop the connection before its topology response.
            writer.write(encode_lan_frame(payload))
            await writer.drain()
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    async def receive(message):
        if not recovered.done():
            recovered.set_result(message)

    async with connected_runtime(peer, receive, endpoint) as runtime:
        await runtime.async_get_topology()
        assert await asyncio.wait_for(recovered, 4) == payload
        assert [request["method"] for request in requests] == [method, method]
        assert requests[0]["id"] != requests[1]["id"]
