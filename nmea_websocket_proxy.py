#!/usr/bin/env python3
"""
NMEA AIS WebSocket Proxy Server
Bridges TCP NMEA streams to WebSocket clients
"""

import asyncio
import websockets
import socket
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Set, Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NMEAWebSocketProxy:
    def __init__(self, ws_port: int = 8080, allowed_origins: Set[str] = None):
        self.ws_port = ws_port
        self.allowed_origins = allowed_origins or {'*'}
        self.active_connections: Dict[websockets.WebSocketServerProtocol, dict] = {}
        self.tcp_tasks: Dict[websockets.WebSocketServerProtocol, asyncio.Task] = {}

    async def handle_client(self, websocket):
        """Handle new WebSocket client connection"""
        client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
        logger.info(f"WebSocket client connected from {client_ip}")
        
        self.active_connections[websocket] = {
            'ip': None,
            'port': None,
            'connected': False,
            'start_time': datetime.now(),
            'client_ip': client_ip
        }

        try:
            # Send welcome message
            await websocket.send(json.dumps({
                'type': 'welcome',
                'message': 'NMEA WebSocket Proxy Server Connected',
                'timestamp': datetime.now().isoformat()
            }))

            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_message(websocket, data)
                except json.JSONDecodeError:
                    await self.send_error(websocket, "Invalid JSON format")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    await self.send_error(websocket, f"Server error: {str(e)}")

        except websockets.exceptions.ConnectionClosedError:
            logger.info(f"WebSocket client {client_ip} disconnected")
        except Exception as e:
            logger.error(f"WebSocket error for client {client_ip}: {e}")
        finally:
            await self.cleanup_connection(websocket)

    async def handle_message(self, websocket, data: dict):
        """Handle incoming WebSocket messages"""
        message_type = data.get('type')
        
        if message_type == 'connect':
            ip = data.get('ip')
            port = data.get('port')
            if ip and port:
                await self.connect_tcp_source(websocket, ip, int(port))
            else:
                await self.send_error(websocket, "Missing IP or port")
                
        elif message_type == 'disconnect':
            await self.disconnect_tcp_source(websocket)
            
        elif message_type == 'ping':
            await websocket.send(json.dumps({
                'type': 'pong',
                'timestamp': datetime.now().isoformat()
            }))
            
        else:
            await self.send_error(websocket, f"Unknown message type: {message_type}")

    async def connect_tcp_source(self, websocket, ip: str, port: int):
        """Connect to TCP NMEA source"""
        # Disconnect existing connection if any
        if websocket in self.tcp_tasks:
            await self.disconnect_tcp_source(websocket)

        logger.info(f"Attempting to connect to TCP source: {ip}:{port}")

        try:
            # Create TCP connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=10.0
            )
            
            logger.info(f"Connected to TCP source: {ip}:{port}")
            
            # Update connection info
            self.active_connections[websocket].update({
                'ip': ip,
                'port': port,
                'connected': True,
                'tcp_writer': writer
            })

            # Send confirmation
            await websocket.send(json.dumps({
                'type': 'connected',
                'ip': ip,
                'port': port,
                'timestamp': datetime.now().isoformat()
            }))

            # Start TCP reading task
            self.tcp_tasks[websocket] = asyncio.create_task(
                self.tcp_reader_task(websocket, reader, writer, ip, port)
            )

        except asyncio.TimeoutError:
            await self.send_error(websocket, f"Connection timeout to {ip}:{port}")
        except ConnectionRefusedError:
            await self.send_error(websocket, f"Connection refused to {ip}:{port}")
        except Exception as e:
            logger.error(f"TCP connection error: {e}")
            await self.send_error(websocket, f"Connection error: {str(e)}")

    async def tcp_reader_task(self, websocket, reader, writer, ip: str, port: int):
        """Task to read from TCP connection and forward to WebSocket"""
        buffer = ""
        
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    logger.info(f"TCP connection closed by server: {ip}:{port}")
                    break
                
                buffer += data.decode('utf-8', errors='ignore')
                
                # Process complete lines
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if line and (line.startswith('!') or line.startswith('$')):
                        # Send NMEA sentence to WebSocket client
                        try:
                            await websocket.send(json.dumps({
                                'type': 'nmea',
                                'sentence': line,
                                'timestamp': datetime.now().isoformat()
                            }))
                        except websockets.exceptions.ConnectionClosedError:
                            logger.info("WebSocket client disconnected during TCP read")
                            break

        except Exception as e:
            logger.error(f"TCP reader error: {e}")
            await self.send_error(websocket, f"TCP read error: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            
            # Notify WebSocket client of disconnection
            try:
                await websocket.send(json.dumps({
                    'type': 'tcp_disconnected',
                    'message': 'TCP connection closed',
                    'timestamp': datetime.now().isoformat()
                }))
            except:
                pass  # WebSocket might be closed

    async def disconnect_tcp_source(self, websocket):
        """Disconnect from TCP source"""
        if websocket in self.tcp_tasks:
            task = self.tcp_tasks[websocket]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.tcp_tasks[websocket]

        connection = self.active_connections.get(websocket, {})
        if 'tcp_writer' in connection:
            writer = connection['tcp_writer']
            writer.close()
            await writer.wait_closed()
            del connection['tcp_writer']

        connection.update({
            'connected': False,
            'ip': None,
            'port': None
        })

        try:
            await websocket.send(json.dumps({
                'type': 'disconnected',
                'message': 'Disconnected from TCP source',
                'timestamp': datetime.now().isoformat()
            }))
        except:
            pass  # WebSocket might be closed

    async def cleanup_connection(self, websocket):
        """Clean up WebSocket connection"""
        await self.disconnect_tcp_source(websocket)
        if websocket in self.active_connections:
            del self.active_connections[websocket]

    async def send_error(self, websocket, message: str):
        """Send error message to WebSocket client"""
        try:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': message,
                'timestamp': datetime.now().isoformat()
            }))
        except:
            pass  # WebSocket might be closed

    async def start_server(self):
        """Start the WebSocket server"""
        logger.info(f"Starting NMEA WebSocket Proxy Server on port {self.ws_port}")
        
        # Start WebSocket server
        server = await websockets.serve(
            self.handle_client,
            "0.0.0.0",  # Listen on all interfaces
            self.ws_port,
            ping_interval=30,
            ping_timeout=10
        )
        
        logger.info(f"NMEA WebSocket Proxy Server listening on ws://0.0.0.0:{self.ws_port}")
        
        # Start stats task
        asyncio.create_task(self.stats_task())
        
        # Wait forever
        await server.wait_closed()

    async def stats_task(self):
        """Periodic stats logging"""
        while True:
            await asyncio.sleep(30)
            active_count = len(self.active_connections)
            connected_count = sum(1 for conn in self.active_connections.values() if conn.get('connected', False))
            logger.info(f"Active WebSocket clients: {active_count}, TCP connections: {connected_count}")

def main():
    parser = argparse.ArgumentParser(description='NMEA AIS WebSocket Proxy Server')
    parser.add_argument('--port', type=int, default=8080, help='WebSocket server port (default: 8080)')
    parser.add_argument('--origins', type=str, help='Comma-separated list of allowed origins (default: all)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Parse allowed origins
    allowed_origins = set(args.origins.split(',')) if args.origins else {'*'}
    
    # Create and start proxy
    proxy = NMEAWebSocketProxy(ws_port=args.port, allowed_origins=allowed_origins)
    
    try:
        asyncio.run(proxy.start_server())
    except KeyboardInterrupt:
        logger.info("Shutting down proxy server...")

if __name__ == "__main__":
    main()

