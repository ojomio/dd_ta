import logging
from urllib.parse import urljoin

from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import IOLoop
import tornado.locks

c = AsyncHTTPClient()
base_url = 'http://turkeytr.net'
sem = tornado.locks.Semaphore(3)

queued_links = set()
timed_out_links = set()
downloaded_links = []
data = {}


@coroutine
def get_async(url, _callback, *args, **kwargs):
    """
    Downloads resource and invokes processing callback with it
    :param url: url of the resource
    :param _callback: function that takes the body and optional
    :param args:
    :param kwargs:
    :return: returns what callback returns
    """

    url = urljoin(base_url, url)
    if url in queued_links:
        logging.warning('%s has already been queued for download' % url)
        return
    queued_links.add(url)
    logging.debug('Queueing %s' % url)
    with (yield sem.acquire()):  # Restrict simultaneous requests to avoid server DOS or ban
        logging.debug('Downloading %s' % url)
        logging.debug('Acquired for %s (%d left)' % (url, sem._value))
        try:
            resp = yield c.fetch(url)
        except HTTPError as e:
            logging.exception('Exception was caught. Response processing cancelled')
            if e.code == 599:  # Timeout
                logging.warning('Timed out. Requeueing %s' % url)
                queued_links.discard(url)
                timed_out_links.add(url)
                IOLoop.current().add_callback(get_async, url, _callback, *args, **kwargs)
            return

    callback_res = _callback(resp, *args, **kwargs)  # pass additional arguments besides response
    try:
        if isinstance(callback_res, Future):
            result = (yield callback_res)
        else:
            result = callback_res
    except Exception:
        raise
    else:
        logging.debug('Done %s' % url)
        downloaded_links.append(url)
        timed_out_links.discard(url)
        return result