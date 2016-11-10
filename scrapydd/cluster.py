from pysyncobj import SyncObj, replicated
import logging
from config import Config

logger = logging.getLogger(__name__)


class ClusterNode():
    def __init__(self, task_id, config=None):
        if config is None:
            config = Config()
        sync_address = config.get('cluster_bind_address')
        sync_ports = config.get('cluster_bind_ports').split(',')
        sync_port = int(sync_ports[task_id])
        peers = config.get('cluster_peers').split(',') if config.get('cluster_peers') else []
        peers = peers + ['%s:%d' % (sync_address, int(port)) for port in sync_ports if int(port) != sync_port]
        logging.debug('starting cluster node.')
        logging.debug('cluster node binding: %s:%d' % (sync_address, sync_port))
        logging.debug('cluster other peers: %s' % peers)
        try:
            self.sync_obj = ClusterSyncObj(sync_address, sync_port, peers)
        except Exception as e:
            logger.error('Error when creating sync_obj')


class ClusterSyncObj(SyncObj):
    def __init__(self, bind_address, bind_port, peers):
        super(ClusterSyncObj, self).__init__('%s:%d' % (bind_address, bind_port), peers)
        self.__counter = 0

    @replicated
    def add_schedule_job(self, trigger_id):
        logger.debug('_add_schedule_job')
        if self.on_add_schedule_job is not None:
            try:
                self.on_add_schedule_job(trigger_id)
            except Exception as e:
                logger.error('Error when adding schedule job : ' + e)

    def set_on_add_schedule_job(self, callback):
        self.on_add_schedule_job = callback

    @replicated
    def remove_schedule_job(self, trigger_id):
        logger.debug('_remove_schedule_job %s' % trigger_id)
        if self.on_remove_schedule_job is not None:
            try:
                self.on_remove_schedule_job(str(trigger_id))
            except Exception as e:
                logger.error('Error when removing schedule job : ' + e)

    def set_on_remove_schedule_job(self, callback):
        self.on_remove_schedule_job = callback
