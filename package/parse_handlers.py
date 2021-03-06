import logging
import re
import urllib.parse

from pyquery import PyQuery
from tornado.gen import coroutine

from package import get_async, rollback_on_exception
from package.geocode import geocode_handler, google_geocode_url, partition
from package.model import session, Address, Firm


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

        deferred_exception = None
        for portion in partition(cat_pages, chunk=2):  # Process 2 pages before queueing next 2
            try:
                yield [
                    get_async(cat_pager_page_url,
                              parse_cat_pager_page,
                              main_cat_title=title
                              )
                    for cat_pager_page_url in portion
                    ]
            except Exception as e:  # catch exception so that the other portions are executed as well
                logging.exception('Exception occurred during %s, skipping to process the next address batch' % url)
                # but raise in the end to indicate overall failure and prevent url from being marked as downloaded
                deferred_exception = e

        if deferred_exception:
            raise Exception('%s is incomplete due to exception in one of its pages.'
                            ' Details follow' % url) from deferred_exception

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

        deferred_exception = None
        # Process 2 pages before queueing next 2(saves memory on futures)
        for portion in partition(sub_cat_pages, chunk=2):
            try:
                yield [
                    get_async(sub_cat_pager_page_url,
                              parse_sub_cat_pager_page,
                              main_cat_title=main_cat_title,
                              sub_cat_title=title,
                              )
                    for sub_cat_pager_page_url in portion
                ]
            except Exception as e:  # catch exception so that the other portions are executed as well
                logging.exception('Exception occurred during %s, skipping to process the next address batch' % url)
                # but raise in the end to indicate overall failure and prevent url from being marked as downloaded
                deferred_exception = e

        if deferred_exception:
            raise Exception('%s is incomplete due to exception in one of its pages.'
                            ' Details follow' % url) from deferred_exception

    logging.info('Subcategory %s>%s done' % (main_cat_title, title))


@coroutine
@rollback_on_exception()
def parse_sub_cat_pager_page(resp, main_cat_title, sub_cat_title):
    pq = PyQuery(resp.body)
    for node in pq('ul.firms > li'):
        try:
            name = (node.xpath('./div[@class="title"]/a')[0].text or '').encode('iso-8859-1').decode('utf8')
            address = (node.xpath('./div[@class="address"]')[0].text or '').encode('iso-8859-1').decode('utf8')
            firm = session.query(Firm).filter_by(name=name).first()
            addr_obj = Address(
                category=main_cat_title,
                subcategory=sub_cat_title,
                firm=firm,
            )
            session.add(addr_obj)

            if not firm:  # Create firm if it wasn't mentioned before
                firm = Firm(name=name, address=address)
                addr_obj.firm = firm

                yield get_async(  # Resolve coordinates and city for its address
                    (google_geocode_url % urllib.parse.quote(firm.address)),
                    geocode_handler,
                    firm=firm,
                )

        except UnicodeEncodeError as e:
            logging.exception('String decoding problem ("%s")' % node.text_content())
