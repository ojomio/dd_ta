import logging
from urllib.parse import urljoin

from pyquery import PyQuery
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.httpclient import HTTPClient, AsyncHTTPClient
from tornado.ioloop import IOLoop
import tornado.locks

c = AsyncHTTPClient()
base_url = 'http://turkeytr.net'
sem = tornado.locks.Semaphore(3)


@coroutine
def get_async(url):
    logging.debug('Queueing %s' % url)
    with (yield sem.acquire()):
        logging.debug('Downloading %s' % url)
        logging.debug('Acquired(%d left)' % sem._value)
        resp = yield c.fetch(urljoin(base_url, url))
        logging.debug('Done %s' % url)
        return resp.body


@coroutine
def chain(future, callback):
    callback_res = callback((yield future))
    if isinstance(callback_res, Future):
        return (yield callback_res)
    else:
        return callback_res


@coroutine
def parse_site():
    main_body = yield get_async('/')
    pq = PyQuery(main_body)
    categories = pq('#top_categories li>h4>a')
    try:
        yield [
            chain(
                get_async(cat.get('href')),
                parse_category
            )
            for cat in categories
            ]
    finally:
        IOLoop.current().stop()


@coroutine
def parse_category(body):
    return


@coroutine
def parse_subcategory():
    pass


@coroutine
def parse_page():
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('tornado.access').setLevel('DEBUG')
    logging.getLogger('tornado.general').setLevel('DEBUG')
    ioloop = IOLoop.current()
    ioloop.add_callback(parse_site)
    ioloop.start()
