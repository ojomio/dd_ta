import json
import logging
import urllib.parse
from signal import SIGINT, alarm, SIGALRM
from signal import signal

from tornado.gen import coroutine
from tornado.ioloop import IOLoop

import package
from package import get_async
from package.model import session, Firm, Address, Locality

google_geocode_url = 'https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyBWBckrECvIQkd3cpwxJ6Qe7EDOF96U91A' \
                     '&address=%s&language=en'


def sigint_handler(sig, trace):
    logging.error('Caught SIGINT. Emergency stopping parser...')
    raise Exception('Stop parser')


def sigalarm_handler(sig, trace):
    save()
    alarm(30)  # schedule next alarm in 10 secs


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
    for portion in partition(
            iterable=session.query(
                Firm
            ).filter(
                Firm.address != None
            ).filter(
                Firm.locality == None
            ),
            chunk=100
    ):

        yield [
            get_async(
                (google_geocode_url % urllib.parse.quote(firm.address)),
                geocode_handler,
                firm=firm,
            )
            for firm in portion
        ]
    ioloop.stop()


@coroutine
def geocode_handler(resp, firm):
    resp = json.loads(resp.body.decode())
    if resp['status'] != "OK":
        logging.info('Error in geocoding firm address %s. Ignoring request' % firm.address)
        logging.error(str(resp))
        return

    coordinates = '{lat} {lng}'.format(**resp['results'][0]['geometry']['location'])  # precise coordinates of the firm
    toponym = ', '.join([
        addr_component['long_name']
        for addr_component in resp['results'][0]['address_components']
        if set(addr_component['types']) & {'country', 'administrative_area_level_1', 'administrative_area_level_2', 'locality'}
    ])

    locality = session.query(Locality).filter_by(locality=toponym).first()
    if not locality:  # Сheck if we have coordinates for the city of firm in interest
        yield get_async(  # if not, get them and store in db
            (google_geocode_url % urllib.parse.quote(toponym)),
            record_new_toponym,
            toponym_name=toponym,
        )

    firm.locality = toponym
    firm.coordinates = coordinates


def record_new_toponym(resp, toponym_name):
    resp = json.loads(resp.body.decode())

    if resp['status'] != "OK":
        logging.info('Error in geocoding toponym address %s. Ignoring request')
        logging.error(str(resp))
        return

    coordinates = '{lat} {lng}'.format(**resp['results'][0]['geometry']['location'])
    session.add(Locality(name=toponym_name,
                         coordinates=coordinates))

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
