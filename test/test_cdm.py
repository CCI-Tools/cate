from unittest import TestCase

from ect.core import DatasetCollection


class CdmTest(TestCase):
    def test_use_case(self):
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


from ect.core.cdm_xarray import XArrayDatasetAdapter


class DatasetCollectionTest(TestCase):
    def test_use_case(self):
        ds1 = XArrayDatasetAdapter({'a': 1})
        ds2 = XArrayDatasetAdapter({'b': 2})
        ds3 = XArrayDatasetAdapter({'c': 3})
        ds4 = XArrayDatasetAdapter({'d': 4})
        ds5 = XArrayDatasetAdapter({'e': 5})

        dsc = DatasetCollection(ds1, ds2, ds3, ozone=ds4, aerosol=ds5)
        self.assertEqual(dsc.datasets, [ds1, ds2, ds3, ds4, ds5])
        self.assertEqual(dsc.wrapped_datasets, [{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}, {'e': 5}])

        removed_ds = dsc.remove_dataset('ozone')
        self.assertIs(removed_ds, ds4)
        self.assertEqual(dsc.datasets, [ds1, ds2, ds3, ds5])
        self.assertEqual(dsc.wrapped_datasets, [{'a': 1}, {'b': 2}, {'c': 3}, {'e': 5}])

        removed_ds = dsc.remove_dataset(ds1)
        self.assertIs(removed_ds, ds1)
        self.assertEqual(dsc.datasets, [ds2, ds3, ds5])
        self.assertEqual(dsc.wrapped_datasets, [{'b': 2}, {'c': 3}, {'e': 5}])

        removed_ds = dsc.remove_dataset(ds3.wrapped_dataset)
        self.assertIs(removed_ds, ds3)
        self.assertEqual(dsc.datasets, [ds2, ds5])
        self.assertEqual(dsc.wrapped_datasets, [{'b': 2}, {'e': 5}])

        removed_ds = dsc.remove_dataset(ds3)
        self.assertIs(removed_ds, None)

