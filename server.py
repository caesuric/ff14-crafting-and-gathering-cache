"""
This is the main entrypoint for the server.
"""
import asyncio
from lib.web_services import start_web_server

if __name__=='__main__':
    print('Starting server.')
    asyncio.run(start_web_server())
