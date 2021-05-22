import configparser
import sys

from loguru import logger


class App(object):

    def __init__(self):
        if len(sys.argv) < 2:
            print("Config not provided")
            exit(1)
        config_name = sys.argv[1]

        # initialize config
        self.config = configparser.ConfigParser()
        # self.config.read(self.__class__.__name__ + '.ini')
        self.config.read(config_name)
        self.app_name = self.config['App']['Name']

        # initialize logger
        self.log_path = self.config['App']['LogPath']
        self.log_level = self.config['App']['LogLevel']
        self.logger = logger
        self.logger.add(self.log_path + "/" + self.app_name + "_{time}.log")

    def start(self):
        pass