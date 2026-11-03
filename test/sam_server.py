#!/usr/bin/env python3
"""Minimal SAM bridge mock for the response-buffer lifetime regression test."""

import socket
import socketserver


class SamHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.request.settimeout(10)
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
                response = b"SESSION STATUS RESULT=OK\n"
            elif command == "NAMING LOOKUP NAME=ME":
                response = b"NAMING REPLY RESULT=OK NAME=ME VALUE=local-dest\n"
            else:
                return

            try:
                self.wfile.write(response)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                return


if __name__ == "__main__":
    with socketserver.TCPServer(("127.0.0.1", 0), SamHandler) as server:
        print(f"LISTENING_PORT {server.server_address[1]}", flush=True)
        server.serve_forever(poll_interval=0.1)
