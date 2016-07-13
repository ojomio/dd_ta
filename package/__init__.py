import functools
import logging
from types import GeneratorType
from urllib.parse import urljoin, urlparse

import tornado.locks
from sqlalchemy.exc import SQLAlchemyError
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient, HTTPError

from package.model import VisitedLink
from .model import session

c = AsyncHTTPClient(max_clients=100)
base_url = 'http://turkeytr.net'
semaphors = {'turkeytr.net': tornado.locks.Semaphore(20),
             'maps.googleapis.com': tornado.locks.Semaphore(70)}

queued_links = set()
timed_out_links = set()


def rollback_on_exception(suppress_exception=False):
    '''
    Roll tx back if we cannot continue
    :param suppress_exception:
    :return:
    '''
    def deco (fn):
        def wrapper_gen(underlying_gen):
            try:
                yield from underlying_gen
            except SQLAlchemyError:
                logging.exception('SQL exception @%r' % fn)
                session.rollback()
                if not suppress_exception:
                    raise

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                res = fn(*args, **kwargs)
                if isinstance(res, GeneratorType):
                    return wrapper_gen(res)
                else:
                    return res
            except SQLAlchemyError:
                logging.exception('SQL exception @%r' % fn)
                session.rollback()
                if not suppress_exception:
                    raise
        return wrapper
    return deco


@coroutine
def get_async(url, _callback, attempts=5, check_queued=True, *args, **kwargs):
    """
    Downloads resource and invokes processing callback with it
    :param url: url of the resource
    :param _callback: function that takes the body and optional
    :param args:
    :param kwargs:
    :return: returns what callback returns
    """

    url = urljoin(base_url, url)
    domain = urlparse(url)[1]
    sem = semaphors[domain]

    if check_queued and url in queued_links:
        logging.warning('%s has already been queued for download' % url)
        return
    queued_links.add(url)
    logging.debug('Queueing %s' % url)
    for attempt in range(attempts):
        with (yield sem.acquire()):  # Restrict simultaneous requests to avoid server DOS or ban
            logging.debug('Downloading %s' % url)
            logging.debug('Acquired for %s (%d left)' % (url, sem._value))

            try:
                resp = yield c.fetch(url, request_timeout=600)
                break
            except HTTPError as e:  # propagate any other error
                logging.exception('Exception was caught. Response processing cancelled')
                if e.code == 599:  # Timeout
                    logging.warning('Timed out. Requeueing %s' % url)
                    timed_out_links.add(url)
                    if attempt == attempts-1:  # last attempt and still timeout
                        # In case someone wants to request this URL from another place, we don't mind repeating
                        queued_links.discard(url)
                        return  # Just give up trying and skip this URL
                else:
                    raise  # propagate any other HTTP error

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
        session.add(VisitedLink(link=url))
        timed_out_links.discard(url)
        return result
