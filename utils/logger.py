import logging
import loguru
import configparser


class Logger(loguru.Logger):
    def __init__(self, config):
        app_name = config['App']['Name']
        log_path = config['App']['LogPath']
        self.add(log_path + "/" + app_name + "_{time}.log")
