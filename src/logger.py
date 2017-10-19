

import logging


class Logger:

    def __init__(self, level=logging.INFO):
        logfmt = "[%(asctime)s - %(levelname)s] - %(message)s"
        dtfmt = "%Y-%m-%d %I:%M:%S"
        logging.basicConfig(level=level, format=logfmt, datefmt=dtfmt)

    def get(self):
        return logging.getLogger()