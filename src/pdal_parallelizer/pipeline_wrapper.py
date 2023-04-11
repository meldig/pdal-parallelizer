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

    def get_writers(self):
        return list(filter(lambda x: x["type"].startswith("writers"), self.loaded_pipeline))

    def add_crop_filter(self, bounds):
        crop_filter = '{"type": "filters.crop"}'
        crop_filter_parsed = json.loads(crop_filter)
        crop_filter_parsed["bounds"] = bounds
        self.loaded_pipeline.insert(1, crop_filter_parsed)