__author__ = 'oleg'
import logging

from logging import handlers
import time

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')

# logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.INFO)

loggers = {
    'putin': {
        'errors': {"logger": logging.getLogger('putin_errors')},
        'info': {"logger": logging.getLogger('putin_info')}
    },

    'vk': {
        'errors': {"logger": logging.getLogger('vk_errors')},
        'info': {"logger": logging.getLogger('vk_info')}
    },
    'lj': {
        'errors': {"logger": logging.getLogger('lj_errors')},
        'info': {"logger": logging.getLogger('lj_info')}
    },
    'rss': {
        'errors': {"logger": logging.getLogger('rss_errors')},
        'info': {"logger": logging.getLogger('rss_info')}
    },
    'website': {
        'errors': {"logger": logging.getLogger('website_errors')},
        'info': {"logger": logging.getLogger('website_info')}
    },
    'links': {
        'errors': {"logger": logging.getLogger('links_errors')},
        'info': {"logger": logging.getLogger('links_info')}
    },
    'cleanup': {
        'errors': {"logger": logging.getLogger('cleanup_errors')},
        'info': {"logger": logging.getLogger('cleanup_info')}
    },
    'messages': {
        'errors': {"logger": logging.getLogger('messages_errors')},
        'info': {"logger": logging.getLogger('messages_info')}
    },
    'replication': {
        'errors': {"logger": logging.getLogger('replication_errors')},
        'info': {"logger": logging.getLogger('replication_info')}
    },
    'unknown': {
        'errors': {"logger": logging.getLogger('unknown_errors')},
        'info': {"logger": logging.getLogger('unknown_info')}
    },
    'debug': {
        'errors': {"logger": logging.getLogger('debug')},
        'debug': {"logger": logging.getLogger('debug')},
        'info': {"logger": logging.getLogger('debug')}
    }

}

today = time.strftime('%Y%m%d')
formatter = logging.Formatter(LOG_FORMAT)
for logger_name in loggers:
    for logger_type in loggers[logger_name]:
        loggers[logger_name][logger_type]["logger"].setLevel(logging.DEBUG)
        loggers[logger_name][logger_type]["file"] = handlers.WatchedFileHandler(
                'logs/' + today + '_' + logger_name + '_' + logger_type + '.log', encoding='utf-8')
        loggers[logger_name][logger_type]["console"] = logging.StreamHandler()
        loggers[logger_name][logger_type]["file"].setFormatter(formatter)
        loggers[logger_name][logger_type]["console"].setFormatter(formatter)
        loggers[logger_name][logger_type]["logger"].addHandler(loggers[logger_name][logger_type]["file"])
        loggers[logger_name][logger_type]["logger"].addHandler(loggers[logger_name][logger_type]["console"])

