

import logging


class Logger:

    def __init__(self, level='INFO'):
        logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
        dtfmt = "%Y-%m-%d %I:%M:%S"

        if level == 'ERROR':
            loglv = logging.ERROR
        elif level == 'DEBUG':
            loglv = logging.DEBUG
        else:
            loglv = logging.INFO

        logging.basicConfig(level=loglv, format=logfmt, datefmt=dtfmt)

    def get(self):
        return logging.getLogger()
