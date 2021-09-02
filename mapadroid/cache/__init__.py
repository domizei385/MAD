from mapadroid.cache.noopcache import NoopCache
from mapadroid.utils.logging import LoggerEnums, get_logger

logger = get_logger(LoggerEnums.system)


def get_cache(args):
    return get_custom_cache(args.enable_cache, args.cache_host, args.cache_port, args.cache_database)


def get_custom_cache(enabled, host, port, database):
    cache = NoopCache()
    if enabled:
        try:
            import redis
            cache = redis.Redis(host=host, port=port, db=database)
            cache.ping()
        except ImportError:
            logger.error("Cache enabled but redis dependency not installed. Continuing without cache")
        except redis.exceptions.ConnectionError:
            logger.error("Unable to connect to Redis server. Continouing without cache")
        except Exception:
            logger.error("Unknown error while enabling cache. Continuing without cache")

    return cache
