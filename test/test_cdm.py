from unittest import TestCase

from ect.core.cdm import DatasetCollection
from ect.core.cdm import Schema
from ect.core.cdm_xarray import XArrayDatasetAdapter


class CdmTest(TestCase):
    def test_basic_idea(self):
        xarray_ds = {}
        shapefile_ds = {}

        collection = DatasetCollection()

        collection.add_xarray_dataset(xarray_ds)
        collection.add_shapefile_dataset(shapefile_ds)

        self.assertEqual(len(collection.wrapped_datasets), 2)
        self.assertEqual(len(collection.datasets), 2)

        subset = collection.subset(spatial_roi=(0, 0, 1, 1))

        self.assertEqual(len(subset.wrapped_datasets), 2)

        subset.close()

        collection.remove_dataset(xarray_ds)
        collection.remove_dataset(shapefile_ds)

        self.assertEqual(len(collection.wrapped_datasets), 0)

        collection.close()


class DatasetCollectionTest(TestCase):
    def test_use_case(self):
        ds1 = XArrayDatasetAdapter('A')
        ds2 = XArrayDatasetAdapter('B')
        ds3 = XArrayDatasetAdapter('C')
        ds4 = XArrayDatasetAdapter('D')
        ds5 = XArrayDatasetAdapter('E')

        dsc = DatasetCollection(ds1, ds2, ds3, ozone=ds4, aerosol=ds5)
        self.assertEqual(set(dsc.datasets), {ds1, ds2, ds3, ds4, ds5})
        self.assertEqual(set(dsc.wrapped_datasets), {'A', 'B', 'C', 'D', 'E'})

        removed_ds = dsc.remove_dataset('ozone')
        self.assertIs(removed_ds, ds4)
        self.assertEqual(set(dsc.datasets), {ds1, ds2, ds3, ds5})
        self.assertEqual(set(dsc.wrapped_datasets), {'A', 'B', 'C', 'E'})

        removed_ds = dsc.remove_dataset(ds1)
        self.assertIs(removed_ds, ds1)
        self.assertEqual(set(dsc.datasets), {ds2, ds3, ds5})
        self.assertEqual(set(dsc.wrapped_datasets), {'B', 'C', 'E'})

        removed_ds = dsc.remove_dataset(ds3.wrapped_dataset)
        self.assertIs(removed_ds, ds3)
        self.assertEqual(set(dsc.datasets), {ds2, ds5})
        self.assertEqual(set(dsc.wrapped_datasets), {'B', 'E'})

        removed_ds = dsc.remove_dataset(ds3)
        self.assertIs(removed_ds, None)


import json


class SchemaTest(TestCase):
    @staticmethod
    def _test_schema() -> Schema:
        return Schema('test',
                      dimensions=[Schema.Dimension('lon', length=720),
                                  Schema.Dimension('lat', length=360),
                                  Schema.Dimension('time', length=12)],
                      variables=[Schema.Variable('SST', float, dimension_names=['lon', 'lat', 'time']),
                                 Schema.Variable('qc_flags', int, dimension_names=['lon', 'lat', 'time'])],
                      attributes=[Schema.Attribute('title', str, 'Sea Surface Temperature')])

    def test_rank_and_dim(self):
        schema = self._test_schema()

        self.assertEqual(schema.dimension(2).name, 'time')
        self.assertEqual(schema.dimension('lat').name, 'lat')

        sst = schema.variables[0]
        self.assertEqual(sst.rank, 3)
        self.assertEqual(sst.dimension(schema, 0).name, 'lon')
        self.assertEqual(sst.dimension(schema, 1).name, 'lat')
        self.assertEqual(sst.dimension(schema, 2).name, 'time')

    def test_to_and_from_json(self):
        schema_1 = self._test_schema()

        json_dict_1 = schema_1.to_json_dict()
        json_text_1 = json.dumps(json_dict_1, indent=2)
        # print(json_text_1)

        schema_2 = Schema.from_json_dict(json_dict_1)
        json_dict_2 = schema_2.to_json_dict()
        json_text_2 = json.dumps(json_dict_2, indent=2)
        # print(json_text_2)

        self.maxDiff = None
        self.assertEqual(json_text_1, json_text_2)
