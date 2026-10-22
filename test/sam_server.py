#!/usr/bin/env python3
"""Minimal SAM 3.1 mock: SESSION CREATE, NAMING LOOKUP, STREAM CONNECT.

STREAM CONNECT switches the same TCP socket into a bidirectional byte tunnel
to the already running websocket_server.py. No real I2P router is used.
"""

import asyncio
import sys

TRACKER_PORT = int(sys.argv[1])
FAIL_LOOKUP = bool(int(sys.argv[2]))
DESTINATION = "mock-i2p-destination"


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        hello = await reader.readline()
        if not hello.startswith(b"HELLO VERSION "):
            return
        writer.write(b"HELLO REPLY RESULT=OK VERSION=3.1\n")
        await writer.drain()
        while True:
            command = await reader.readline()
            if not command:
                return
            if command.startswith(b"SESSION CREATE "):
                writer.write(
                    b"SESSION STATUS RESULT=OK DESTINATION="
                    + DESTINATION.encode()
                    + b"\n"
                )
            elif command.startswith(b"NAMING LOOKUP "):
                name = command.split(b"NAME=", 1)[1].strip()
                if name == b"ME":
                    writer.write(
                        b"NAMING REPLY RESULT=OK NAME=ME VALUE="
                        + DESTINATION.encode()
                        + b"\n"
                    )
                elif FAIL_LOOKUP:
                    writer.write(b"NAMING REPLY RESULT=KEY_NOT_FOUND\n")
                else:
                    writer.write(
                        b"NAMING REPLY RESULT=OK NAME="
                        + name
                        + b" VALUE="
                        + DESTINATION.encode()
                        + b"\n"
                    )
            elif command.startswith(b"STREAM ACCEPT "):
                # The client keeps an incoming SAM listener open in parallel
                # with its control session. No incoming peer is needed here.
                writer.write(b"STREAM STATUS RESULT=OK\n")
                await writer.drain()
                await reader.read()
                return
            elif command.startswith(b"STREAM CONNECT "):
                remote_reader, remote_writer = await asyncio.open_connection(
                    "127.0.0.1", TRACKER_PORT
                )
                writer.write(b"STREAM STATUS RESULT=OK\n")
                await writer.drain()
                await asyncio.gather(
                    pipe(reader, remote_writer), pipe(remote_reader, writer)
                )
                return
            else:
                return
            await writer.drain()
    except (ConnectionError, OSError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass


async def main() -> None:
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    print("LISTENING_PORT " + str(server.sockets[0].getsockname()[1]), flush=True)
    async with server:
        await server.serve_forever()


asyncio.run(main())
