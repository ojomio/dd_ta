import json
import logging
import urllib.parse
from itertools import takewhile
from signal import SIGINT, alarm, SIGALRM, SIGUSR1
from signal import signal
from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import sessionmaker
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from package.model import Base, VisitedLink, session, Address
from package.parse_handlers import parse_site
from package import get_async
import package

google_geocode_url = 'https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyBWBckrECvIQkd3cpwxJ6Qe7EDOF96U91A' \
                     '&address=%s&language=ru'


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
                distinct(Address.address)
            ).filter(
                        Address.address != None
            ),
            chunk=100
    ):

        yield [
            get_async(
                (google_geocode_url % urllib.parse.quote(addr_string[0])),
                geocode_handler,
                addr_string=addr_string[0]
            )
            for addr_string in portion
        ]
    ioloop.stop()


def geocode_handler(resp, addr_string):
    resp = json.loads(resp.body)
    if resp['status'] != "OK":
        logging.info('Error. Ignoring request')
        logging.error(str(resp))

    toponym = ', '.join([
        addr_component['long_name']
        for addr_component in resp['results'][0]['address_components']
        if set(addr_component['types']) & {'administrative_area_level_1', 'administrative_area_level_2', 'locality'}
    ])
    coordinates = '{lat} {lng}'.format(resp['results'][0]['geometry']['location'])
    session.query(Address).filter_by(address=addr_string).update(
        locality=toponym,
        coordinates=coordinates,
    )


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
