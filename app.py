import configparser
from loguru import logger


class App(object):

    def __init__(self):
        # initialize config
        self.config = configparser.ConfigParser()
        self.config.read(self.__class__.__name__ + '.ini')
        self.app_name = self.config['App']['Name']

        # initialize logger
        self.log_path = self.config['App']['LogPath']
        self.log_level = self.config['App']['LogLevel']
        self.logger = logger
        self.logger.add(self.log_path + "/" + self.app_name + "_{time}.log")
