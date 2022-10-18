import unittest

import src.pdal_parallelizer.bounds as bounds
import src.pdal_parallelizer.cloud as cloud
import src.pdal_parallelizer.do as do
import src.pdal_parallelizer.tile as tile
import src.pdal_parallelizer.file_manager as file_manager
import pdal
import json
import os
import dask
from dask.distributed import Client


class TestBounds(unittest.TestCase):
    def test_getDistX(self):
        data = bounds.Bounds(10, 10, 30, 30)
        result = data.getDistX()
        self.assertEqual(result, 20)

    def test_getDistY(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.getDistY()
        self.assertEqual(result, 40)

    def test_positive_buffer(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.buffer(10)
        self.assertIsInstance(result, tuple)
        coord = result[1]
        self.assertEqual(coord.minx, 0)
        self.assertEqual(coord.miny, 0)
        self.assertEqual(coord.maxx, 40)
        self.assertEqual(coord.maxy, 60)

    def test_negative_buffer(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.buffer(-10)
        self.assertIsInstance(result, tuple)
        coord = result[1]
        self.assertEqual(coord.minx, 0)
        self.assertEqual(coord.miny, 0)
        self.assertEqual(coord.maxx, 40)
        self.assertEqual(coord.maxy, 60)


class TestCloud(unittest.TestCase):
    def test_getCount(self):
        data = cloud.Cloud("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz")
        result = data.getCount()
        self.assertEqual(result, 10)

    def test_bounds(self):
        b = bounds.Bounds(10, 10, 20, 40)
        data = cloud.Cloud("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz", b)
        result = data.bounds
        self.assertEqual(result.minx, b.minx)
        self.assertEqual(result.miny, b.miny)
        self.assertEqual(result.maxx, b.maxx)
        self.assertEqual(result.maxy, b.maxy)

    def test_hasClassFlags(self):
        data = cloud.Cloud("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz")
        result = data.hasClassFlags()
        self.assertTrue(result)


class TestDo(unittest.TestCase):
    with open("D:/data_unit_test/pdal-parallelizer/pipeline.json", "r") as p:
        pipeline = json.load(p)
    pipeline = [pdal.Pipeline(json.dumps(pipeline)), 'temp_name']

    client = Client()

    def test_process(self):
        dask.compute(do.process(self.pipeline))
        nb_files = len([name for name in os.listdir("D:/data_unit_test/pdal-parallelizer/output")])
        self.assertGreater(nb_files, 0)

    def test_process_exc(self):
        fun = dask.compute(do.process)
        self.assertRaises(FileNotFoundError, fun[0], self.pipeline, "D:/data_unit_test/pdal-parallelizer/temp")

    def test_serializePipeline(self):
        dask.compute(do.process(self.pipeline))
        do.serializePipeline(self.pipeline, "D:/data_unit_test/pdal-parallelizer/temp")
        nb_files = len([name for name in os.listdir("D:/data_unit_test/pdal-parallelizer/temp")])
        self.assertGreater(nb_files, 0)

    def test_process_delete_serialized_pipeline(self):
        do.serializePipeline(self.pipeline, "D:/data_unit_test/pdal-parallelizer/temp")
        nb_files_before = len([name for name in os.listdir("D:/data_unit_test/pdal-parallelizer/temp")])
        dask.compute(do.process(self.pipeline, "D:/data_unit_test/pdal-parallelizer/temp"))
        nb_files_after = len([name for name in os.listdir("D:/data_unit_test/pdal-parallelizer/temp")])
        self.assertEqual(nb_files_before - 1, nb_files_after)

    def test_process_serialized_pipelines(self):
        do.serializePipeline(self.pipeline, "D:/data_unit_test/pdal-parallelizer/temp")
        iterator = iter(self.pipeline[1])
        delayed = do.process_serialized_pipelines("D:/data_unit_test/pdal-parallelizer/temp", iterator)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("D:/data_unit_test/pdal-parallelizer/output",
                                       "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                                       iterator,
                                       "D:/data_unit_test/pdal-parallelizer/temp",
                                       False,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_singlefile(self):
        t = tile.Tile("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz",
                      "D:/data_unit_test/pdal-parallelizer/output", "D:/data_unit_test/pdal-parallelizer/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("D:/data_unit_test/pdal-parallelizer/output",
                                       "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                                       iterator,
                                       "D:/data_unit_test/pdal-parallelizer/temp",
                                       False,
                                       True)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("D:/data_unit_test/pdal-parallelizer/output",
                                       "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                                       iterator,
                                       "D:/data_unit_test/pdal-parallelizer/temp",
                                       True,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_singlefile(self):
        t = tile.Tile("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz",
                      "D:/data_unit_test/pdal-parallelizer/output", "D:/data_unit_test/pdal-parallelizer/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("D:/data_unit_test/pdal-parallelizer/output",
                                       "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                                       iterator,
                                       "D:/data_unit_test/pdal-parallelizer/temp",
                                       True,
                                       True)

        self.assertGreater(len(delayed), 0)

    def test_splitCloud(self):
        result = do.splitCloud("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz",
                               "D:/data_unit_test/pdal-parallelizer/output",
                               "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                               (1000, 1000)
                               )
        self.assertIsNotNone(result)


class TestFileManager(unittest.TestCase):
    def test_getFiles_all(self):
        result = file_manager.getFiles("D:/data_unit_test/pdal-parallelizer/input")
        self.assertGreater(len(list(next(result))), 3)

    def test_getFiles_nFiles(self):
        result = file_manager.getFiles("D:/data_unit_test/pdal-parallelizer/input", 2)
        self.assertEqual(len(list(result)), 2)

    def test_getSerializedPipelines(self):
        result = file_manager.getSerializedPipelines("D:/data_unit_test/pdal-parallelizer/temp")
        self.assertGreater(len(list(result)), 0)


class TestTile(unittest.TestCase):
    t = tile.Tile("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz",
                  "D:/data_unit_test/pdal-parallelizer/output",
                  "D:/data_unit_test/pdal-parallelizer/pipeline.json",
                  bounds=bounds.Bounds(685019.31, 7047019.02, 685019.93, 7047019.98),
                  cloud_object=cloud.Cloud("D:/data_unit_test/pdal-parallelizer/input/echantillon_10pts.laz"))

    def test_pipeline(self):
        result = self.t.pipeline(False)
        self.assertIsInstance(result[0], pdal.Pipeline)
        self.assertEqual(result[1], "temp__echantillon_10pts")

    def test_split(self):
        result = self.t.split(0.30, 0.30)
        self.assertEqual(len(list(result)), 12)


if __name__ == "__main__":
    unittest.main()
