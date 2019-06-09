import logging
import os
from logging.config import dictConfig
from pykamq.connection import Connection, RABBITMQ_MANAGER

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s | %(levelname)s | %(process)d | %(thread)d | %(filename)s:%('
                      'lineno)d | %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'simple': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        '__main__': {
            'level': 'DEBUG',
            'handlers': ['console', ],
            'propagate': True,
        },
        'pykamq.connection': {
            'level': 'DEBUG',
            'handlers': ['console', ],
            'propagate': True,
        },
    },
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger(__name__)

RABBIT_MQ = {
    'default': {
        'HOST': 'localhost',
        'PORT': 5672,
        'USER': 'guest',
        'PASSWORD': 'guest',
        'VHOST': 'pykamq',
        'OPTIONS': {
            'heartbeat': 20,
            'connection_attempts': 3,
        }
    }
}
if __name__ == "__main__":
    RABBITMQ_MANAGER.set_configs(RABBIT_MQ)
    consumer = RABBITMQ_MANAGER.get_consumer('default')
    publisher = RABBITMQ_MANAGER.get_publisher('default')
    print(id(consumer))
    print(id(publisher))
    print('Done')
