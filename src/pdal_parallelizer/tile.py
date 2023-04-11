class Tile:
    def __init__(self, name, cloud, bounds, buffer=None):
        self.name = name
        self.cloud = cloud
        self.bounds = bounds
        self.buffer = buffer if buffer else None

