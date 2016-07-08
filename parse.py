import json
import logging
import re
from signal import SIGINT, alarm, SIGALRM, SIGUSR1
from signal import signal
from urllib.parse import urljoin

import tornado.locks
from pyquery import PyQuery
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import IOLoop

c = AsyncHTTPClient()
base_url = 'http://turkeytr.net'
sem = tornado.locks.Semaphore(3)
queued_links = set()
timed_out_links = set()
downloaded_links = []
data = {}


@coroutine
def get_async(url, _callback, *args, **kwargs):
    url = urljoin(base_url, url)
    if url in queued_links:
        logging.warning('%s has already been queued for download' % url)
        return
    queued_links.add(url)
    logging.debug('Queueing %s' % url)
    with (yield sem.acquire()):
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


@coroutine
def parse_site(resp):
    pq = PyQuery(resp.body)
    categories = pq('#top_categories li>h4>a')
    yield [
        get_async(cat.get('href'), parse_category)
        for cat in categories
        if not re.search(
            r'turkish-manufacturers-companies-list|manufacturers-companies-turkey|'
            r'turkishcompanies|producer-companies-list-turkey|suppliers-companies-turkey|'
            r'made-in-turkey|istanbul-companies-turkey',
            cat.get('href')
        )
    ]
    # stop io loop if all processing is done or an exception is thrown and propagated up the coroutine chain


@coroutine
def parse_category(resp, ):
    url = resp.effective_url
    pq = PyQuery(resp.body)
    title = pq('h1')[0].text.strip()
    yield parse_cat_pager_page(resp, main_cat_title=title)  # Process the current page as category pager page#1

    cat_pages = []
    # the last is the Next button, so also skip it
    try:
        max_page = int(pq('div.pages_nav > a')[-2].text)
    except IndexError:
        # 1 page in category
        pass
    else:
        for page_idx in range(2, max_page + 1):  # +1 to include max_page
            cat_page_url = re.sub(r'/([^/]*)\.htm$', r'/\1/\1_pg-%d.html' % page_idx, url)
            cat_pages.append(cat_page_url)
        yield [
            get_async(cat_pager_page_url,
                      parse_cat_pager_page,
                      main_cat_title=title
                      )
            for cat_pager_page_url in cat_pages
        ]
    logging.info('Category %s done' % (title))


@coroutine
def parse_cat_pager_page(resp, main_cat_title):
    url = resp.effective_url
    pq = PyQuery(resp.body)
    yield [
        get_async(subcat_node.get('href'),
                  parse_subcategory,
                  main_cat_title=main_cat_title,
                  )
        for subcat_node in pq('ul.prds > li > a')
    ]
    logging.info('Category %s page %s done' % (main_cat_title, url))


@coroutine
def parse_subcategory(resp, main_cat_title):
    url = resp.effective_url
    pq = PyQuery(resp.body)
    title = pq('h1')[0].text.strip()
    yield parse_sub_cat_pager_page(resp, main_cat_title=main_cat_title,
                                   sub_cat_title=title)  # Process the current page as subcategory pager page#1
    sub_cat_pages = []
    # the last is the Next button, so also skip it
    try:
        max_page = int(pq('div.pages_nav > a')[-2].text)
    except IndexError:
        # 1 page in sub category
        pass
    else:
        for page_idx in range(2, max_page + 1):  # +1 to include max_page
            sub_cat_page_url = re.sub(r'\.html$', (r'_page-%d.html' % page_idx), url)
            sub_cat_pages.append(sub_cat_page_url)
        yield [
            get_async(sub_cat_pager_page_url,
                      parse_sub_cat_pager_page,
                      main_cat_title=main_cat_title,
                      sub_cat_title=title,
                      )
            for sub_cat_pager_page_url in sub_cat_pages
        ]
    logging.info('Subcategory %s>%s done' % (main_cat_title, title))


@coroutine
def parse_sub_cat_pager_page(resp, main_cat_title, sub_cat_title):
    pq = PyQuery(resp.body)
    for node in pq('ul.firms > li'):
        data.setdefault(main_cat_title, {}).setdefault(sub_cat_title, []).append({
            'name': node.xpath('./div[@class="title"]/a')[0].text,
            'address': node.xpath('./div[@class="address"]')[0].text,
        })


def sigint_handler(sig, trace):
    logging.error('Caught SIGINT. Emergency stopping parser...')
    raise Exception('Stop parser')


def sigalarm_handler(sig, trace):
    save()
    alarm(10)


def sigusr_handler(sig, trace):
    logging.info('Timed out:')
    logging.info(timed_out_links)
    logging.info('Downloaded:')
    logging.info(downloaded_links)
    logging.info('Queued:')
    logging.info(queued_links)


def save():
    logging.info('Saving state...')
    with open('./viewed.json', 'w') as fp:
        json.dump(downloaded_links, fp, indent=True)
    with open('./adresses.json', 'w') as fp:
        json.dump(data, fp, indent=True)
        # with open('./timed_out.json', 'w') as fp:
        #     json.dump(list(timed_out_links), fp, indent=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('tornado.access').setLevel('DEBUG')
    logging.getLogger('tornado.general').setLevel('DEBUG')

    try:  # Try to pull what was saved earlier
        with open('./viewed.json') as fp:
            downloaded_links = json.load(fp)
            queued_links = set(downloaded_links)  # do not queue what is already downloaded
        with open('./adresses.json') as fp:
            data = json.load(fp)
            # with open('./timed_out.json') as fp:
            #     timed_out_links = json.load(fp)
    except (ValueError, OSError):
        pass

    ioloop = IOLoop.current()
    ioloop.add_future(get_async('/', parse_site), lambda future: ioloop.stop())

    signal(SIGALRM, sigalarm_handler)
    alarm(10)
    signal(SIGINT, sigint_handler)
    signal(SIGUSR1, sigusr_handler)
    try:
        ioloop.start()
    finally:
        save()

    logging.info('Timed out:')
    logging.info(timed_out_links)
