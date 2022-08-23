import logging
import sys

formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
no_console = False


class LevelFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, log_record):
        return log_record.levelno >= self.__level


def setup_logger(name, log_file, level=logging.INFO, erase=True):
    if erase:
        # delete previous log files
        with open(log_file, 'w'):
            pass

    handler_f = logging.FileHandler(log_file)
    handler_f.setFormatter(formatter)
    if not no_console:
        handler_c = logging.StreamHandler(sys.stdout)
        handler_c.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler_f)
    if not no_console:
        logger.addHandler(handler_c)

    return logger


main_logger = setup_logger('main', 'logs/main.log', erase=False)
loggers = ['load_backbone', 'nsr_species_match', 'load_bold', 'specimen', 'marker', 'synonym', 'load_klasse']
for logger_name in loggers:
    setup_logger(logger_name, f'logs/{logger_name}.log', erase=False)

