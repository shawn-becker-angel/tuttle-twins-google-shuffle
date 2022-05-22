import logging

def set_all_info_loggers_to_debug_level():
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        if logger.getEffectiveLevel() == logging.INFO:
            logger.setLevel(logging.DEBUG)
