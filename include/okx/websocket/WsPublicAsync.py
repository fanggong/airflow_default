import asyncio
import json
import logging
import websockets

from okx.websocket.WebSocketFactory import WebSocketFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WsPublic")


class WsPublicAsync:
    def __init__(self, url):
        self.websocket = None
        self.url = url
        self.subscriptions = set()
        self.callback = None
        self.loop = asyncio.get_event_loop()
        self.factory = WebSocketFactory(url)

    async def connect(self):
        self.websocket = await self.factory.connect()

    async def consume(self):
        try:
            async for message in self.websocket:
                logger.info("Received message: {%s}", message)
                if self.callback:
                    self.callback(message)
        except websockets.exceptions.ConnectionClosedError:
            await self.stop()
            # await self.stop()
            # await asyncio.sleep(2)
            # await self.start()


    async def subscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "subscribe",
            "args": params
        })
        await self.websocket.send(payload)

    async def unsubscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "unsubscribe",
            "args": params
        })
        logger.info(f"unsubscribe: {payload}")
        await self.websocket.send(payload)

    async def stop(self):
        await self.factory.close()
        self.loop.stop()

    async def start(self):
        logger.info("Connecting to WebSocket...")
        await self.connect()
        self.loop.create_task(self.consume())

    def stop_sync(self):
        self.loop.run_until_complete(self.stop())
