import pdal
import json


class PipelineWrapper:
    def __init__(self, pipeline):
        with open(pipeline, "r") as p:
            self.pipeline = p
            self.loaded_pipeline = json.load(p)
            self.pdal_pipeline = pdal.Pipeline(json.dumps(self.loaded_pipeline))

    def get_json(self) -> str:
        return self.pdal_pipeline.toJSON()

    def get_readers(self):
        return list(filter(lambda x: x["type"].startswith("readers"), self.loaded_pipeline))

    def set_readers_filename(self, filename):
        self.get_readers()[0]["filename"] = filename

    def get_writers(self):
        return list(filter(lambda x: x["type"].startswith("writers"), self.loaded_pipeline))

    def set_writers_filename(self, filename):
        self.get_writers()[0]["filename"] = filename

    def add_crop_filter(self, bounds):
        crop_filter = '{"type": "filters.crop"}'
        crop_filter_parsed = json.loads(crop_filter)
        crop_filter_parsed["bounds"] = f"([{bounds.min_x}, {bounds.max_x}], [{bounds.min_y}, {bounds.max_y}])"
        self.loaded_pipeline.insert(1, crop_filter_parsed)

    def add_ClassFlags(self):
        ferry_filter = '{"type": "filters.ferry", "dimensions": "=>ClassFlags"}'
        ferry_filter_parsed = json.loads(ferry_filter)
        self.loaded_pipeline.insert(1, ferry_filter_parsed)
