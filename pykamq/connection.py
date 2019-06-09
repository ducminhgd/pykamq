"""A module for manage connections of RabbitMQ
    Consider RabbitMQ settings is a dictionary like this

    RABBIT_MQ = {
        'connection_name': {
            'AMQP_URI': 'amqp://user:password@host:port/vhost',
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
"""
import os

import pika
import logging

logger = logging.getLogger(__name__)

AMQP_URI_KEY = "AMQP_URI"
HOST_KEY = "HOST"
PORT_KEY = "PORT"
USER_KEY = "USER"
PASSWORD_KEY = "PASSWORD"
VHOST_KEY = "VHOST"
OPTIONS_KEY = "OPTIONS"


class Connection:
    """Connection, wraps pika.SelectConnection"""

    __params = None
    __connection = None

    def __init__(
            self,
            amqp_uri: str = None,
            host: str = "127.0.0.1",
            port: int = 5672,
            user: str = "guest",
            password: str = "guest",
            vhost: str = "/",
            **options,
    ):
        if amqp_uri is not None:
            self.__params = pika.URLParameters(amqp_uri)
        else:
            credentials = pika.PlainCredentials(
                username=user, password=password
            )
            self.__params = pika.ConnectionParameters(
                credentials=credentials,
                host=host,
                port=port,
                virtual_host=vhost,
                **options,
            )
        self.__connection = pika.SelectConnection(
            parameters=self.__params,
            on_open_callback=self.on_open_callback,
            on_open_error_callback=self.on_open_error_callback,
            on_close_callback=self.on_close_callback,
        )

    def on_open_callback(self, *args, **kwags):
        """Can be overrode"""
        logger.info(
            f"Connection opened: "
            f"{self.__params._host}:{self.__params._port}"
            f"/{self.__params._virtual_host}"
        )

    def on_open_error_callback(self, *args, **kwags):
        """Can be overrode"""
        logger.error(
            f"Connection failed: "
            f"{self.__params._host}:{self.__params._port}"
            f"/{self.__params._virtual_host}"
        )

    def on_close_callback(self, *args, **kwags):
        """Can be overrode"""
        logger.info(
            f"Connection closed: "
            f"{self.__params._host}:{self.__params._port}"
            f"/{self.__params._virtual_host}"
        )

    @property
    def is_closed(self):
        return self.__connection.is_closed

    @property
    def is_open(self):
        return self.__connection.is_open

    def start(self):
        try:
            self.__connection.ioloop.start()
        except KeyboardInterrupt:
            # Gracefully close the connection
            self.__connection.close()
            # Loop until we're fully closed, will stop on its own
            self.__connection.ioloop.start()

    def close(self):
        self.__connection.close()


class ConnectionManager:
    """Manages connections by separate them into connections for consumers
        and connections for publishers
    """

    __publishers: dict = None
    __consumers: dict = None
    __configs: dict = None

    def __init__(self, configs: dict = None):
        """
        Create manager with custome configurations
        :param configs:
        """
        self.set_configs(configs)
        self.__consumers = {}
        self.__publishers = {}

    def set_configs(self, configs: dict = None):
        """Sets configurations for manager"""
        if configs is None or not any(configs):
            configs = {}
        self.__configs = configs

    def get_consumer(self, name: str) -> Connection:
        """
        Gets consumer's connection
            if it does not exist then create one
            if it is closed then reconnect
        :param name: name of config
        :return: connection of consumer
        """
        if name not in self.__consumers or self.__consumers[name].is_closed:
            params = self.__configs.get(name)
            if params is None or not any(params):
                return None
            con = Connection(
                amqp_uri=params.get(AMQP_URI_KEY, None),
                host=params.get(HOST_KEY, None),
                port=params.get(PORT_KEY, None),
                user=params.get(USER_KEY, None),
                password=params.get(PASSWORD_KEY, None),
                vhost=params.get(VHOST_KEY, None),
                **params.get(OPTIONS_KEY, {})
            )
            self.__consumers[name] = con

        return self.__consumers[name]

    def get_publisher(self, name: str) -> Connection:
        """
        Gets publisher's connection
            if it does not exist then create one
            if it is closed then reconnect
        :param name: name of config
        :return: connection of publisher
        """
        if name not in self.__publishers or self.__publishers[name].is_closed:
            params = self.__configs.get(name)
            if params is None or not any(params):
                return None
            con = Connection(
                amqp_uri=params.get(AMQP_URI_KEY, None),
                host=params.get(HOST_KEY, None),
                port=params.get(PORT_KEY, None),
                user=params.get(USER_KEY, None),
                password=params.get(PASSWORD_KEY, None),
                vhost=params.get(VHOST_KEY, None),
                **params.get(OPTIONS_KEY, {})
            )
            self.__publishers[name] = con

        return self.__publishers[name]


RABBITMQ_MANAGER = ConnectionManager()
