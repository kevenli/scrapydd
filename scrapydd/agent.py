from .executor import Executor
import logging
import sys
from .daemonize import daemonize
import signal
import os
from .config import AgentConfig
from optparse import OptionParser
import subprocess
import tornado
import scrapydd
from six.moves import input
from six.moves.urllib.parse import urlparse, urljoin, urlencode
from .security import authenticated_request
from tornado.httpclient import HTTPClient, HTTPError
import json
from six.moves.configparser import SafeConfigParser
from .client import get_client

logger = logging.getLogger(__name__)

def init_logging(config):
    import logging.handlers
    logger = logging.getLogger()
    if not os.path.exists('logs'):
        os.mkdir('logs')
    fh = logging.handlers.TimedRotatingFileHandler('logs/scrapydd-agent.log', when='D', backupCount=7)
    ch = logging.StreamHandler()
    eh = logging.handlers.TimedRotatingFileHandler(os.path.join('logs/scrapydd-agent-error.log'), when='D',
                                                   backupCount=30)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    eh.setFormatter(formatter)
    eh.setLevel(logging.ERROR)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(eh)

    if config.getboolean('debug'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

class Daemon():
    def __init__(self, pidfile):
        self.pidfile = pidfile
        self.subprocess_p = None
        self.pid = 0

    def start_subprocess(self):
        argv = sys.argv
        argv.remove('--daemon')
        pargs = argv
        env = os.environ.copy()
        self.subprocess_p = subprocess.Popen(pargs, env=env)
        self.subprocess_p.wait()

    def read_pidfile(self):
        try:
            with open(self.pidfile, 'r') as f:
                return int(f.readline())
        except IOError:
            return None

    def try_remove_pidfile(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def on_signal(self, signum, frame):
        print('closing')
        if self.subprocess_p:
            self.subprocess_p.terminate()
        self.try_remove_pidfile()
        tornado.ioloop.IOLoop.instance().stop()

    def start(self):
        print('Starting scrapydd agent daemon.')
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        daemonize(pidfile=self.pidfile)
        start()
        self.try_remove_pidfile()

def run(argv=None):
    if argv is None:
        argv = sys.argv
    parser = OptionParser(prog  = 'scrapydd agent')
    parser.add_option('-g', '--register', action='store_true', help='register this agent.')
    parser.add_option('--daemon', action='store_true', help='run scrapydd agent in daemon mode')
    parser.add_option('--pidfile', help='pid file will be created when daemon started, \
default: scrapydd-agent.pid')
    opts, args = parser.parse_args(argv)

    import asyncio
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    pidfile = opts.pidfile or 'scrapydd-agent.pid'

    if opts.register:
        run_register()
    elif opts.daemon:
        daemon = Daemon(pidfile=pidfile)
        daemon.start()
    else:
        start()

def run_register():
    key = input('Please input node key:')
    key_secret = input('Please input node key_secret:')
    config = AgentConfig()
    server_base = config.get('server')
    client = get_client(config, app_key=key, app_secret=key_secret)
    try:
        response_data = client.register_node(key)
        if not os.path.exists('conf'):
            os.makedirs('conf')
        with open('conf/node.conf', 'w') as f:
            cp = SafeConfigParser()
            cp.add_section('agent')
            cp.set('agent', 'node_id', str(response_data['id']))
            cp.set('agent', 'node_key', key)
            cp.set('agent', 'secret_key', key_secret)
            cp.write(f)
        print('Register succeed!')

    except HTTPError as e:
        print("Error when registering.")
        print(e.message)


def start():
    config = AgentConfig()
    init_logging(config)
    logging.info('------------------------')
    logging.info('Starting scrapydd agent.')
    logging.info('config %s loaded' % config._loaded_files)
    logging.info('scrapydd version : %s' % scrapydd.__version__)
    logging.info('------------------------')
    executor = Executor(config)
    executor.start()

if __name__ == '__main__':
    run()