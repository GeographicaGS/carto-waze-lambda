from src.logger import Logger


class Model:
    def __init__(self, verbose):
        self._logger = self.load_logger(verbose)

    def load_logger(self, verbose):
        if not verbose:
            lg = Logger(level='ERROR')
        else:
            lg = Logger()

        return lg.get()
