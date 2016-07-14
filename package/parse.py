#!/usr/bin/env python3

import logging
from signal import SIGINT, alarm, SIGALRM, SIGUSR1
from signal import signal

from tornado.ioloop import IOLoop

import package
from package import get_async
from package.model import VisitedLink, session
from package.parse_handlers import parse_site


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


# If there is an error during save, we don't want it to stop the parsing process
@package.rollback_on_exception(suppress_exception=True)
def save():
    logging.info('Saving state...')
    session.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
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
