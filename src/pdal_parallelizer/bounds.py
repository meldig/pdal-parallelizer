class Bounds:
    def __init__(self, min_x, min_y, max_x, max_y):
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y

    def get_dist_x(self) -> float:
        return self.max_x - self.min_x

    def get_dist_y(self) -> float:
        return self.max_y - self.min_y

    def __str__(self):
        return f"([{self.min_x:.2f},{self.max_x:.2f}],[{self.min_y:.2f},{self.max_y:.2f}])"