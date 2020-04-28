import logging

class LogMixin(object):
    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

class Base(object):
    pass

if __name__ == '__main__':
    # Configure only in your main program clause
    logging.basicConfig(level=logging.DEBUG,
                        format='%(name)s %(levelname)s %(message)s')

    main()

