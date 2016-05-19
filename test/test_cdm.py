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

        subset = collection.subset(spatial_roi=(0,0,1,1))

        self.assertEqual(len(subset.wrapped_datasets), 2)

        subset.close()

        collection.remove_dataset(xarray_ds)
        collection.remove_dataset(shapefile_ds)

        self.assertEqual(len(collection.wrapped_datasets), 0)

        collection.close()

