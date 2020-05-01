import logging
import csv


class LogMixin(object):
    @property
    def logger(self):
        # name = '.'.join([__name__, self.__class__.__name__])
        class_name = self.__class__.__name__
        return logging.getLogger('kb_Msuite.' + class_name)


class Base(object):
    pass


class TSVMixin(object):

    def init_tsv_writer(self, open_file_handle):

        return csv.writer(
            open_file_handle,
            delimiter='\t',
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL
        )

# if __name__ == '__main__':
#     # Configure only in your main program clause
#     logging.basicConfig(
#         level=logging.DEBUG,
#         format='%(name)s %(levelname)s %(message)s\n')
#
#     main()
