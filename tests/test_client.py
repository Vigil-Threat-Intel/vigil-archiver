import asyncio

from aiohttp import web

from src.client import StreamClient


def test_parse_line():
    parse = StreamClient.parse_line
    assert parse(b'data: {"a": 1}\n') == '{"a": 1}'
    assert parse(b'data:{"a": 1}\r\n') == '{"a": 1}'
    assert parse(b": keepalive\n") is None
    assert parse(b": connected\n") is None
    assert parse(b"\n") is None
    assert parse(b"data:\n") is None
    assert parse(b"event: x\n") is None


async def serve(handler):
    app = web.Application()
    app.add_routes([web.get("/stream", handler)])
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}/stream"


def test_receives_data_lines_and_skips_comments():
    received = []

    async def handler(request):
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b": connected\n\ndata: one\n\n: keepalive\n\ndata: two\n\n")
        await asyncio.sleep(10)
        return response

    async def go():
        runner, url = await serve(handler)
        client = StreamClient(url, lambda alert: on_alert(alert), reconnect_delay=0.05)

        async def on_alert(alert):
            received.append(alert)

        task = asyncio.create_task(client.run())
        await asyncio.sleep(0.5)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()

    asyncio.run(go())
    assert received == ["one", "two"]


def test_reconnects_after_server_closes_the_stream():
    connections = []

    async def handler(request):
        connections.append(1)
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"data: x\n\n")
        return response

    async def go():
        runner, url = await serve(handler)

        async def on_alert(alert):
            pass

        task = asyncio.create_task(StreamClient(url, on_alert, reconnect_delay=0.05).run())
        await asyncio.sleep(0.6)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()

    asyncio.run(go())
    assert len(connections) >= 3


def test_reconnects_when_nothing_is_received_before_timeout():
    connections = []

    async def handler(request):
        connections.append(1)
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await asyncio.sleep(30)
        return response

    async def go():
        runner, url = await serve(handler)

        async def on_alert(alert):
            pass

        client = StreamClient(url, on_alert, reconnect_delay=0.05, read_timeout=0.2)
        task = asyncio.create_task(client.run())
        await asyncio.sleep(1.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()

    asyncio.run(go())
    assert len(connections) >= 2


def test_keeps_retrying_when_server_is_down():
    async def go():
        async def on_alert(alert):
            pass

        client = StreamClient("http://127.0.0.1:9/stream", on_alert, reconnect_delay=0.05)
        task = asyncio.create_task(client.run())
        await asyncio.sleep(0.4)
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
