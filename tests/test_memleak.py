import logging
from scrapydd.executor import Executor

logger = logging.getLogger(__name__)

def main():
    # code for debuging memory leak
    import objgraph
    from pympler import muppy, summary

    def check_memory():
        logger.debug('Checking memory.')
        logger.debug(objgraph.by_type('SpiderTask'))
        logger.debug(objgraph.by_type('TaskExecutor'))
        logger.debug(objgraph.by_type('Future'))
        logger.debug(objgraph.by_type('PeriodicCallback'))
        logger.debug(objgraph.by_type('Workspace'))
        logger.debug(objgraph.by_type('MultipartRequestBodyProducer'))
        logger.debug(objgraph.by_type('HTTPRequest'))
        future_objs = objgraph.by_type('Future')
        if future_objs:
            objgraph.show_chain(
                objgraph.find_backref_chain(future_objs[-1], objgraph.is_proper_module),
                filename='chain.png'
            )

        all_objects = muppy.get_objects()
        sum1 = summary.summarize(all_objects)
        # Prints out a summary of the large objects
        summary.print_(sum1)

    check_memory_callback = PeriodicCallback(check_memory, 10 * 1000)
    check_memory_callback.start()

    executor = Executor()
    executor.start()

if __name__ == '__main__':
    main()