import unittest
from src.pdal_parallelizer import cloud, bounds, file_manager, tile
import pdal
import json
import os
import dask
from dask.distributed import Client


class TestBounds(unittest.TestCase):
    def test_getDistX(self):
        bds = bounds.Bounds(10, 10, 30, 30)
        result = bds.get_dist_x()
        self.assertEqual(result, 20)

    def test_getDistY(self):
        bds = bounds.Bounds(10, 10, 30, 50)
        result = bds.get_dist_y()
        self.assertEqual(result, 40)


class TestCloud(unittest.TestCase):
    def test_getCount(self):
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        result = cld.get_num_points()
        self.assertEqual(result, 10)

    def test_bounds(self):
        b = bounds.Bounds(10, 10, 20, 40)
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz", b)
        result = cld.bounds
        self.assertEqual(result.min_x, b.min_x)
        self.assertEqual(result.min_y, b.min_y)
        self.assertEqual(result.max_x, b.max_x)
        self.assertEqual(result.max_y, b.max_y)

    def test_has_ClassFlags_dimension(self):
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        result = cld.has_ClassFlags_dimension()
        self.assertTrue(result)

    def test_split(self):
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        tiles = cld.split((0.3, 0.3), "../test/data/pipeline.json", "../test/data/output")
        self.assertEqual(len(tiles), 12)

    def test_split_2(self):
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        tiles = cld.split((100, 100), "../test/data/pipeline.json", "../test/data/output")
        self.assertEqual(len(tiles), 1)

    def test_split_3(self):
        cld = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        tiles = cld.split((0.3, 0.3), "../test/data/pipeline.json", "../test/data/output", 10)
        self.assertEqual(len(tiles), 10)


"""
class TestDo(unittest.TestCase):
    with open("test/data/pipeline.json", "r") as p:
        pipeline = json.load(p)
    pipeline = [pdal.Pipeline(json.dumps(pipeline)), 'temp_name']

    client = Client()

    def test_process(self):
        dask.compute(do.process(self.pipeline))
        nb_files = len([name for name in os.listdir("test/data/output")])
        self.assertGreater(nb_files, 0)

    def test_serializePipeline(self):
        dask.compute(do.process(self.pipeline))
        do.serializePipeline(self.pipeline, "test/data/temp")
        nb_files = len([name for name in os.listdir("test/data/temp")])
        self.assertGreater(nb_files, 0)

    def test_process_delete_serialized_pipeline(self):
        do.serializePipeline(self.pipeline, "test/data/temp")
        nb_files_before = len([name for name in os.listdir("test/data/temp")])
        dask.compute(do.process(self.pipeline, "test/data/temp"))
        nb_files_after = len([name for name in os.listdir("test/data/temp")])
        self.assertEqual(nb_files_before - 1, nb_files_after)

    def test_process_serialized_pipelines(self):
        do.serializePipeline(self.pipeline, "test/data/temp")
        iterator = iter(self.pipeline[1])
        delayed = do.process_serialized_pipelines("test/data/temp", iterator)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       False,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_singlefile(self):
        t = tile.Tile("test/data/input/echantillon_10pts.laz",
                      "../test/data/output", "../test/data/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       False,
                                       True)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       True,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_singlefile(self):
        t = tile.Tile("test/data/input/echantillon_10pts.laz",
                      "../test/data/output", "../test/data/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       True,
                                       True)

        self.assertGreater(len(delayed), 0)

    def test_splitCloud(self):
        result = do.splitCloud("test/data/input/echantillon_10pts.laz",
                               "../test/data/output",
                               "../test/data/pipeline.json",
                               (1000, 1000)
                               )
        self.assertIsNotNone(result)
"""


class TestFileManager(unittest.TestCase):
    def test_getFiles_all(self):
        result = file_manager.getFiles("../test/data/input")
        self.assertGreater(len(list(next(result))), 3)

    def test_getFiles_nFiles(self):
        result = file_manager.getFiles("../test/data/input", 2)
        self.assertEqual(len(list(result)), 2)

    def test_getSerializedPipelines(self):
        result = file_manager.getSerializedPipelines("../test/data/temp")
        self.assertGreater(len(list(result)), 0)


class TestTile(unittest.TestCase):
    bds = bounds.Bounds(685019.31, 7047019.02, 685019.93, 7047019.98)
    cld = cloud.Cloud("../test/data/input/echantillon_10pts.laz", bds)
    t = tile.Tile("t1",
                  cld,
                  bds,
                  "../test/data/pipeline.json",
                  "../test/data/output",
                  (10, 5),
                  False
                  )

    def test_pipeline(self):
        result = self.t.link_pipeline(False)
        self.assertIsInstance(result, pdal.Pipeline)

    def test_positive_buffer(self):
        self.t.add_buffer()
        self.assertEqual(self.t.bounds.min_x, 685009.31)
        self.assertEqual(self.t.bounds.min_y, 7047014.02)
        self.assertEqual(self.t.bounds.max_x, 685029.93)
        self.assertEqual(self.t.bounds.max_y, 7047024.98)


if __name__ == "__main__":
    unittest.main()
