"""
This is the main entrypoint for the server.
"""
import asyncio
from lib.crafting_and_gathering_scraper import start_crafting_and_gathering_scraper
from lib.database_engine import init_database
from lib.web_services import start_web_server

if __name__=='__main__':
    print('Starting server.')
    engine = init_database()
    start_crafting_and_gathering_scraper(engine)
    asyncio.run(start_web_server(engine))
