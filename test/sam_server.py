#!/usr/bin/env python3
"""Minimal SAM v3 mock for the incoming INVALID_ID recovery integration test."""

import socket
import socketserver
import threading
from typing import cast


class SamServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), SamHandler)
        self.lock = threading.Lock()
        self.session_ids: list[str] = []
        self.accepts = 0


class SamHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        server = cast(SamServer, self.server)
        self.request.settimeout(15)
        while True:
            try:
                line = self.rfile.readline()
            except socket.timeout:
                return
            if not line:
                return

            command = line.decode("ascii", errors="replace").strip()
            if command.startswith("HELLO "):
                response = b"HELLO REPLY RESULT=OK VERSION=3.1\n"
            elif command.startswith("SESSION CREATE "):
                session_id = next(
                    (part[3:] for part in command.split() if part.startswith("ID=")),
                    "",
                )
                with server.lock:
                    server.session_ids.append(session_id)
                response = b"SESSION STATUS RESULT=OK\n"
            elif command.startswith("NAMING LOOKUP "):
                response = b"NAMING REPLY RESULT=OK NAME=ME VALUE=local-dest\n"
            elif command.startswith("STREAM ACCEPT "):
                with server.lock:
                    server.accepts += 1
                    first_accept = server.accepts == 1
                response = (
                    b"STREAM STATUS RESULT=INVALID_ID\n"
                    if first_accept
                    else b"STREAM STATUS RESULT=OK\n"
                )
            elif command == "TEST STATUS":
                with server.lock:
                    ids = server.session_ids
                    recovered = (
                        len(ids) >= 2
                        and server.accepts >= 2
                        and bool(ids[0])
                        and ids[0] != ids[1]
                    )
                response = b"1" if recovered else b"0"
            else:
                return

            try:
                self.wfile.write(response)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                return

            if command == "TEST STATUS":
                return


if __name__ == "__main__":
    with SamServer() as server:
        print(f"LISTENING_PORT {server.server_address[1]}", flush=True)
        server.serve_forever(poll_interval=0.1)
