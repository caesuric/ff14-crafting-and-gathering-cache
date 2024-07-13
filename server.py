"""
Runs a web server for retrieving gathering and crafting data.
"""
# pylint: disable=abstract-method
import asyncio
import json
import tornado.httpserver
from tornado.web import Application, RequestHandler

botany_items = []
mining_items = []
fishing_items = []
crafting_items = {}

def load_json_file(file_name, variable):
    """
    Loads a JSON file into the specified list.
    """
    with open(f'data/{file_name}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        variable.extend(data)

def load_crafting_items(file_name):
    """
    Loads crafting items from a JSON file.
    """
    with open(f'data/{file_name}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        for k,v in data.items():
            crafting_items[k] = v

async def main():
    """
    Main function that starts the web server.
    """
    load_json_file('botany', botany_items)
    load_json_file('mining', mining_items)
    load_json_file('fishing', fishing_items)
    load_crafting_items('crafting')
    application = Application([
        (r'^/rest/botany-items/(.+)-(.+)$', BotanyItemsHandler),
        (r'^/rest/mining-items/(.+)-(.+)$', MiningItemsHandler),
        (r'^/rest/fishing-items/(.+)-(.+)$', FishingItemsHandler),
        (r'^/rest/crafting-items/(.*)/(.+)-(.+)$', CraftingItemsHandler),
        (r'^/rest/crafting-types/$', CraftingTypesHandler),
        (r'^/rest/botany-items-count/(.+)-(.+)$', BotanyItemsCountHandler),
        (r'^/rest/mining-items-count/(.+)-(.+)$', MiningItemsCountHandler),
        (r'^/rest/fishing-items-count/(.+)-(.+)$', FishingItemsCountHandler),
        (r'^/rest/crafting-items-count/(.*)/(.+)-(.+)$', CraftingItemsCountHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(application, ssl_options=dict(certfile="/etc/letsencrypt/live/xivmarketstats.com/fullchain.pem", keyfile="/etc/letsencrypt/live/xivmarketstats.com/privkey.pem"))
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

    def options(self, *args):
        """
        Sets options for the request.
        """
        self.set_status(204)
        self.finish()

class BotanyItemsHandler(BaseHandler):
    """
    Request handler for botany items.
    """
    def get(self, min_level, max_level):
        """
        Retrieves botany items within a specified level range.
        """
        self.write(json.dumps(grab_items_for_level_range(botany_items, min_level, max_level)))

class MiningItemsHandler(BaseHandler):
    """
    Request handler for mining items.
    """
    def get(self, min_level, max_level):
        """
        Retrieves mining items within a specified level range.
        """
        self.write(json.dumps(grab_items_for_level_range(mining_items, min_level, max_level)))

class FishingItemsHandler(BaseHandler):
    """
    Request handler for fishing items.
    """
    def get(self, min_level, max_level):
        """
        Retrieves fishing items within a specified level range
        """
        self.write(json.dumps(grab_items_for_level_range(fishing_items, min_level, max_level)))

class CraftingItemsHandler(BaseHandler):
    """
    Request handler for crafting items
    """
    def get(self, crafting_type, min_level, max_level):
        """
        Retrieves crafting items within a specified level range.
        """
        self.write(
            json.dumps(
                grab_items_for_level_range(
                    crafting_items[crafting_type],
                    min_level,
                    max_level
                )
            )
        )

class CraftingTypesHandler(BaseHandler):
    """
    Request handler for crafting types.
    """
    def get(self):
        """
        Retrieves crafting types.
        """
        self.write(json.dumps(list(crafting_items.keys())))

class BotanyItemsCountHandler(BaseHandler):
    """
    Request handler for botany item count.
    """
    def get(self, min_level, max_level):
        """
        Retrieves the count of botany items within a specified level range.
        """
        self.write(str(len(grab_items_for_level_range(botany_items, min_level, max_level))))

class MiningItemsCountHandler(BaseHandler):
    """
    Request handler for mining item count.
    """
    def get(self, min_level, max_level):
        """
        Retrieves the count of mining items within a specified level range.
        """
        self.write(str(len(grab_items_for_level_range(mining_items, min_level, max_level))))

class FishingItemsCountHandler(BaseHandler):
    """
    Request handler for fishing item count.
    """
    def get(self, min_level, max_level):
        """
        Retrieves the count of fishing items within a specified level range.
        """
        self.write(str(len(grab_items_for_level_range(fishing_items, min_level, max_level))))

class CraftingItemsCountHandler(BaseHandler):
    """
    Request handler for crafting item count.
    """
    def get(self, crafting_type, min_level, max_level):
        """
        Retrieves the count of crafting items within a specified level range.
        """
        self.write(
            str(
                len(
                    grab_items_for_level_range(
                        crafting_items[crafting_type],
                        min_level,
                        max_level
                    )
                )
            )
        )

if __name__=='__main__':
    print('Starting server.')
    asyncio.run(main())
