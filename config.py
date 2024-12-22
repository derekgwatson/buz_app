class Config:
    DEBUG = False
    TESTING = False
    SECRET_KEY = "supersecretkey"


class ProductionConfig(Config):


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
