import logging


class MyLogger:

    def log_file(self, filename = "test2.log"):
        logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        return logging

