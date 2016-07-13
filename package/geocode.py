import json
import logging
import urllib.parse
from signal import SIGINT, alarm, SIGALRM
from signal import signal

import re
from tornado.gen import coroutine
from tornado.ioloop import IOLoop

import package
from package import get_async, rollback_on_exception
from package.model import session, Firm, Locality

google_geocode_url = 'https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyBWBckrECvIQkd3cpwxJ6Qe7EDOF96U91A' \
                     '&address=%s&language=en'


def sigint_handler(sig, trace):
    logging.error('Caught SIGINT. Emergency stopping parser...')
    raise Exception('Stop parser')


def sigalarm_handler(sig, trace):
    save()
    alarm(30)  # schedule next alarm in 10 secs


# If there is an error during save, we don't want it to stop the parsing process
@rollback_on_exception(suppress_exception=True)
def save():
    logging.info('Saving state...')
    session.commit()


def partition(iterable, chunk):
    closed = False

    it = iter(iterable)
    def internal():
        nonlocal closed
        try:
            for c in range(chunk):
                yield next(it)
        except StopIteration:
            closed = True
            raise

    while not closed:
        yield internal()


@coroutine
def geocode(ioloop):
    try:
        for portion in partition(
                iterable=session.query(
                    Firm
                ).filter(
                    Firm.address != ''
                ).filter(
                    Firm.address != None
                ).filter(
                    Firm.locality == None
                ),
                chunk=100
        ):
            try:
                yield [
                    get_async(
                        (google_geocode_url % urllib.parse.quote(re.sub('[\r\n]', ' ', firm.address))),
                        geocode_handler,
                        check_queued=False,

                        firm=firm,
                    )
                    for firm in portion
                ]

            except Exception as e:
                logging.exception('Exception occurred, skipping to next address batch ')
    finally:
        ioloop.stop()


@coroutine
@package.rollback_on_exception()
def geocode_handler(resp, firm, try_fix_address=True):
    resp = json.loads(resp.body.decode())
    # Handle Google API errors
    if resp['status'] == "ZERO_RESULTS" \
            and try_fix_address and 'turkey' not in firm.address.lower():

        # Try appending 'Turkey' if no results found for original address
        yield get_async(
            (google_geocode_url % urllib.parse.quote('%s , Turkey' % firm.address)),
            geocode_handler,
            check_queued=False,

            firm=firm,
            try_fix_address=False  # Avoid recursion
        )
        return

    if resp['status'] != "OK":
        logging.info('Error in geocoding firm address %s. Ignoring request' % firm.address)
        logging.error(str(resp))
        firm.locality = '<UNKNOWN>'
        return

    coordinates = '{lat} {lng}'.format(**resp['results'][0]['geometry']['location'])  # precise coordinates of the firm
    firm.coordinates = coordinates

    toponym_preliminary = ', '.join([
        addr_component['long_name']
        for addr_component in resp['results'][0]['address_components']
        if set(addr_component['types']) & {'country', 'administrative_area_level_1', 'administrative_area_level_2', 'locality'}
    ])

    yield get_async(  # Normalize the toponym (create if new)
        (google_geocode_url % urllib.parse.quote(toponym_preliminary)),
        record_new_toponym,
        check_queued=False,

        firm=firm,
        toponym_preliminary=toponym_preliminary,
    )




@package.rollback_on_exception()
def record_new_toponym(resp, firm, toponym_preliminary):
    '''
    Normalize toponym name so that eg Üsküdar, Üsküdar, İstanbul, Turkey =  Üsküdar, İstanbul, Turkey
    Save if new
    :param resp:
    :param firm:
    :param toponym_preliminary: toponym extracted from initial address
    :return:
    '''
    resp = json.loads(resp.body.decode())

    if resp['status'] != "OK":
        logging.info('Error in geocoding toponym address %s')
        logging.error(str(resp))
        firm.locality = toponym_preliminary  # Store at least preliminary toponym (no coords available)
        return

    toponym_normalized = ', '.join([
        addr_component['long_name']
        # store in reverse order for easier search in db
        for addr_component in reversed(resp['results'][0]['address_components'])
        if set(addr_component['types']) & {'country', 'administrative_area_level_1', 'administrative_area_level_2', 'locality'}
    ])
    firm.locality = toponym_normalized

    locality = session.query(Locality).filter_by(locality=toponym_normalized).first()
    if not locality:  # Сheck if we have record for that toponym already
        # coordinates of the toponym's centre
        coordinates = '{lat} {lng}'.format(**resp['results'][0]['geometry']['location'])
        session.add(Locality(locality=toponym_normalized,
                             locality_coordinates=coordinates))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('tornado.access').setLevel('DEBUG')
    logging.getLogger('tornado.general').setLevel('DEBUG')

    ioloop = IOLoop.current()
    ioloop.add_callback(geocode, ioloop)

    # set up signal handlers
    signal(SIGALRM, sigalarm_handler)
    alarm(30)  # send SIGALRM after 10 secs
    signal(SIGINT, sigint_handler)
    try:
        ioloop.start()  # exits after someone calls ioloop.stop()
    finally:
        save()

    logging.info('Timed out:')
    logging.info(package.timed_out_links)
