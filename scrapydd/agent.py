from executor import Executor
import logging
import sys
from daemonize import daemonize
import signal
import os
from config import AgentConfig
from optparse import OptionParser
import subprocess
import tornado
import scrapydd

logger = logging.getLogger(__name__)

def init_logging(config):
    import logging.handlers
    logger = logging.getLogger()
    if not os.path.exists('logs'):
        os.mkdir('logs')
    fh = logging.handlers.TimedRotatingFileHandler('logs/scrapydd-agent.log', when='D', backupCount=7)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

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
        print 'closing'
        if self.subprocess_p:
            self.subprocess_p.terminate()
        self.try_remove_pidfile()
        tornado.ioloop.IOLoop.instance().stop()

    def start(self):
        print 'Starting scrapydd agent daemon.'
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        daemonize(pidfile=self.pidfile)
        start()
        self.try_remove_pidfile()

def run(argv=None):
    if argv is None:
        argv = sys.argv
    parser = OptionParser(prog  = 'scrapydd agent')
    parser.add_option('--daemon', action='store_true', help='run scrapydd agent in daemon mode')
    parser.add_option('--pidfile', help='pid file will be created when daemon started, \
default: scrapydd-agent.pid')
    opts, args = parser.parse_args(argv)

    pidfile = opts.pidfile or 'scrapydd-agent.pid'

    if opts.daemon:
        daemon = Daemon(pidfile=pidfile)
        daemon.start()
    else:
        start()

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