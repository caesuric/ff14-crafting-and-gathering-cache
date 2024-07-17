"""
Runs a web server for retrieving gathering and crafting data.
"""
# pylint: disable=abstract-method
import asyncio
import json
from threading import Thread
from typing import Optional
from uuid import uuid4
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
import tornado.httpserver
from tornado.httputil import HTTPServerRequest
from tornado.web import Application, RequestHandler

from lib.database_engine import CraftingType, ItemData
from lib.universalis_data import (
    get_current_item_data,
    get_historical_item_data,
    get_tax_rates,
    get_worlds
)
from lib.xivapi_data import get_basic_item_data

item_jobs = {}
current_market_jobs = {}
historical_market_jobs = {}

async def start_web_server(engine: Engine):
    """
    Starts the web server.

    Args:
        engine (Engine): SQLAlchemy database engine.
    """
    application = Application([
        (r'^/rest/items/start/(.*)$', ItemsHandler, {'engine': engine}),
        (r'^/rest/items/status/(.*)$', ItemsStatusHandler),
        (r'^/rest/items/result/(.*)$', ItemsResultHandler),
        (r'^/rest/worlds$', WorldsHandler, {'engine': engine}),
        (r'^/rest/tax-rates/(.*)$', TaxRatesHandler, {'engine': engine}),
        (r'^/rest/market-current/start/(.*)/(.*)$', MarketCurrentHandler, {'engine': engine}),
        (r'^/rest/market-current/status/(.*)$', MarketCurrentStatusHandler),
        (r'^/rest/market-current/result/(.*)$', MarketCurrentResultHandler),
        (r'^/rest/market-historical/start/(.*)/(.*)$', MarketHistoricalHandler, {'engine': engine}),
        (r'^/rest/market-historical/status/(.*)$', MarketHistoricalStatusHandler),
        (r'^/rest/market-historical/result/(.*)$', MarketHistoricalResultHandler),
        (r'^/rest/items-by-job/(.*)/(.+)-(.+)$', ItemsByJobHandler, {'engine': engine}),
        (r'^/rest/crafting-types$', CraftingTypesHandler, {'engine': engine}),
    ])
    http_server = tornado.httpserver.HTTPServer(
        application,
        # ssl_options={
        #     'certfile': '/etc/letsencrypt/live/xivmarketstats.com/fullchain.pem',
        #     'keyfile': '/etc/letsencrypt/live/xivmarketstats.com/privkey.pem'
        # }
    )
    http_server.listen(1414)
    await asyncio.Event().wait()

def grab_items_for_level_range(items, min_level, max_level):
    """
    Grabs items within a specified level range.
    """
    min_level = int(min_level)
    max_level = int(max_level)
    return [item['id'] for item in items if min_level <= item['level'] <= max_level]

class BaseHandler(RequestHandler):
    """
    Base handler that enables CORS.
    """
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    # pylint: disable=unused-argument
    def options(self, *args):
        """
        Sets options for the request.
        """
        self.set_status(204)
        self.finish()


class ItemsHandler(BaseHandler):
    """
    Request handler for items.
    """
    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        self.status: Optional[dict] = None
        self.job_id: str = str(uuid4())
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self, raw_item_ids: Optional[str]):
        """
        Starts a job to retrieve a list of items, and returns a job ID.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        if ',' not in raw_item_ids:
            item_ids = [int(raw_item_ids)]
        else:
            item_ids = raw_item_ids.split(',')
        item_ids = [int(item_id) for item_id in item_ids]
        self.write(self.job_id)
        process = Thread(
            target=self.get_async_portion,
            args=(item_ids)
        )
        process.start()

    def get_async_portion(self, item_ids: list[int]):
        """
        Asynchronous portion of the get request.

        Args:
            world (str): World for which to retrieve market data.
            item_ids (list[int]): IDs of the items for which to retrieve market data.
        """
        for state in get_basic_item_data(item_ids, self.engine):
            self.status = state
            item_jobs[self.job_id] = state

class ItemsStatusHandler(BaseHandler):
    """
    Request handler for item job status.
    """
    def get(self, job_id: str):
        """
        Retrieves the status of an item job.
        """
        if job_id not in item_jobs:
            self.set_status(404, 'Job ID not found.')
            self.write(json.dumps({}))
            return
        status = item_jobs[job_id].copy()
        status['items'] = {}
        self.write(json.dumps(status))


class ItemsResultHandler(BaseHandler):
    """
    Request handler for item job results.
    """
    def get(self, job_id: str):
        """
        Retrieves the results of an item job.
        """
        if job_id not in item_jobs:
            self.set_status(404, 'Job ID not found.')
            return
        status = item_jobs[job_id].copy()
        self.write(json.dumps(status['items']))
        if status['complete']:
            del item_jobs[job_id]


class MarketCurrentHandler(BaseHandler):
    """
    Request handler for current market data.
    """

    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        self.status: Optional[dict] = None
        self.job_id: str = str(uuid4())
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self, world: Optional[str], raw_item_ids: Optional[str]):
        """
        Starts a job to retrieve a list of current market data, and returns a job ID.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        if ',' not in raw_item_ids:
            item_ids = [int(raw_item_ids)]
        else:
            item_ids = raw_item_ids.split(',')
        item_ids = [int(item_id) for item_id in item_ids]
        self.write(self.job_id)
        process = Thread(target=self.get_async_portion, args=(world, item_ids))
        process.start()

    def get_async_portion(self, world: str, item_ids: list[int]):
        """
        Asynchronous portion of the get request.

        Args:
            world (str): World for which to retrieve market data.
            item_ids (list[int]): IDs of the items for which to retrieve market data.
        """
        for state in get_current_item_data(world, item_ids, self.engine):
            self.status = state
            current_market_jobs[self.job_id] = state


class MarketCurrentStatusHandler(BaseHandler):
    """
    Request handler for current market job status.
    """

    def get(self, job_id: str):
        """
        Retrieves the status of an item job.
        """
        if job_id not in current_market_jobs:
            self.set_status(404, 'Job ID not found.')
            self.write(json.dumps({}))
            return
        status = current_market_jobs[job_id].copy()
        status['items'] = {}
        self.write(json.dumps(status))


class MarketCurrentResultHandler(BaseHandler):
    """
    Request handler for current market job results.
    """

    def get(self, job_id: str):
        """
        Retrieves the results of an item job.
        """
        if job_id not in current_market_jobs:
            self.set_status(404, 'Job ID not found.')
            return
        status = current_market_jobs[job_id].copy()
        self.write(json.dumps(status['items']))
        if status['complete']:
            del current_market_jobs[job_id]


class MarketHistoricalHandler(BaseHandler):
    """
    Request handler for historical market data.
    """

    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        self.status: Optional[dict] = None
        self.job_id: str = str(uuid4())
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self, world: Optional[str], raw_item_ids: Optional[str]):
        """
        Starts a job to retrieve a list of historical market data, and returns a job ID.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        if ',' not in raw_item_ids:
            item_ids = [int(raw_item_ids)]
        else:
            item_ids = raw_item_ids.split(',')
        item_ids = [int(item_id) for item_id in item_ids]
        self.write(self.job_id)
        process = Thread(target=self.get_async_portion, args=(world, item_ids))
        process.start()

    def get_async_portion(self, world: str, item_ids: list[int]):
        """
        Asynchronous portion of the get request.

        Args:
            world (str): World for which to retrieve market data.
            item_ids (list[int]): IDs of the items for which to retrieve market data.
        """
        for state in get_historical_item_data(world, item_ids, self.engine):
            self.status = state
            historical_market_jobs[self.job_id] = state


class MarketHistoricalStatusHandler(BaseHandler):
    """
    Request handler for current market job status.
    """

    def get(self, job_id: str):
        """
        Retrieves the status of an item job.
        """
        if job_id not in historical_market_jobs:
            self.set_status(404, 'Job ID not found.')
            self.write(json.dumps({}))
            return
        status = historical_market_jobs[job_id].copy()
        status['items'] = {}
        self.write(json.dumps(status))


class MarketHistoricalResultHandler(BaseHandler):
    """
    Request handler for current market job results.
    """

    def get(self, job_id: str):
        """
        Retrieves the results of an item job.
        """
        if job_id not in historical_market_jobs:
            self.set_status(404, 'Job ID not found.')
            return
        status = historical_market_jobs[job_id].copy()
        self.write(json.dumps(status['items']))
        if status['complete']:
            del historical_market_jobs[job_id]


class WorldsHandler(BaseHandler):
    """
    Request handler for world list.
    """
    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self):
        """
        Retrieves the list of worlds.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        self.write(json.dumps(get_worlds(self.engine)))


class TaxRatesHandler(BaseHandler):
    """
    Request handler for world list.
    """
    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self, world: str):
        """
        Retrieves the list of tax rates for a world.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        self.write(json.dumps(get_tax_rates(world, self.engine)))


class ItemsByJobHandler(BaseHandler):
    """
    Request handler for items by job.
    """
    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self, job: Optional[str], min_level: Optional[str], max_level: Optional[str]):
        """
        Retrieves the list of items for a job within a level range.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        if job is None:
            self.set_status(400, 'Job not specified.')
            return
        if min_level is None or max_level is None:
            self.set_status(400, 'Level range not specified.')
            return
        with Session(self.engine) as session:
            db_entries = session.query(ItemData).where(ItemData.source_classes.any(job)).all()
            filtered_entries = []
            for entry in db_entries:
                level = entry.source_class_levels[entry.source_classes.index(job)]
                if int(min_level) <= level <= int(max_level):
                    filtered_entries.append(entry)
            output = [
                {'id': x.id, 'name': x.name, 'icon_path': x.icon_path} for x in filtered_entries
            ]
            self.write(json.dumps(output))


class CraftingTypesHandler(BaseHandler):
    """
    Request handler for crafting types.
    """
    def __init__(self, application: Application, request: HTTPServerRequest, **kwargs):
        super().__init__(application, request, **kwargs)
        self.engine: Optional[Engine] = None
        if 'engine' in kwargs:
            self.engine = kwargs['engine']

    def initialize(self, **kwargs):
        """
        Initializes the handler.
        """
        self.engine = kwargs['engine']

    def get(self):
        """
        Retrieves the list of crafting types.
        """
        if self.engine is None:
            self.set_status(500, 'Database engine not passed to handler.')
            return
        with Session(self.engine) as session:
            db_entries = session.query(CraftingType).all()
            crafting_types = []
            for entry in db_entries:
                if entry.name not in crafting_types:
                    crafting_types.append(entry.name)
            self.write(json.dumps(crafting_types))
