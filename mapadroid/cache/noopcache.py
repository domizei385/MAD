class NoopCache:
    def set(self, key, value, ex=None):
        pass

    def get(self, key):
        pass

    def scan_iter(self, match) -> iter:
        return iter(list())

    def exists(self, key):
        return False
