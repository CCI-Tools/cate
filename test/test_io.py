from unittest import TestCase
import ect.core.io as io

class IOTest(TestCase):

    def test_query_data_sources(self):
        data_sources = io.query_data_sources()
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "sst")

    # def test_open_dataset(self):
    #     ds1 = io.open_dataset(io.DataSource("sst"))
    #     self.assertIsNotNone()
    #     ds2 = io.open_dataset("sst")
