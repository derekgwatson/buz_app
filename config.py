import os


class Config:
    DEBUG = False
    TESTING = False


class ProductionConfig(Config):
    pass


class ProductionConfig(Config):


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
