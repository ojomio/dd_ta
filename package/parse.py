import json
import logging
from signal import SIGINT, alarm, SIGALRM, SIGUSR1
from signal import signal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tornado.ioloop import IOLoop

from package.model import Base, VisitedLink, session
from package.parse_handlers import parse_site
from package import get_async
import package


def sigint_handler(sig, trace):
    logging.error('Caught SIGINT. Emergency stopping parser...')
    raise Exception('Stop parser')


def sigalarm_handler(sig, trace):
    save()
    alarm(30)  # schedule next alarm in 10 secs


def sigusr_handler(sig, trace):
    logging.info('Timed out:')
    logging.info(package.timed_out_links)
    logging.info('Queued:')
    logging.info(package.queued_links)


def save():
    logging.info('Saving state...')
    session.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('tornado.access').setLevel('DEBUG')
    logging.getLogger('tornado.general').setLevel('DEBUG')

    package.queued_links = set([x.link for x in session.query(VisitedLink)])  # do not queue what is already downloaded

    ioloop = IOLoop.current()
    ioloop.add_future(get_async('/', parse_site), lambda future: ioloop.stop())

    # set up signal handlers
    signal(SIGALRM, sigalarm_handler)
    alarm(30)  # send SIGALRM after 10 secs
    signal(SIGINT, sigint_handler)
    signal(SIGUSR1, sigusr_handler)
    try:
        ioloop.start()  # exits after someone calls ioloop.stop()
    finally:
        save()

    logging.info('Timed out:')
    logging.info(package.timed_out_links)
