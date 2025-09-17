# config.py


class Config:
    DEBUG = False
    TESTING = False
    EXPORT_ROOT_SUBDIR = "exports"          # generic, not feature-specific


class ProductionConfig(Config):
    pass


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
