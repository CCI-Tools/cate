# import fiona
# import shapefile

from .cdm import Dataset, DatasetCollection


class ShapefileDataset(Dataset):
    def __init__(self, shapefile):
        super(ShapefileDataset, self).__init__(shapefile)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using fiona or pyshp API
        return self

    def close(self):
        # implement me using fiona or pyshp API
        pass


def add_shapefile_dataset(container: DatasetCollection, shapefile):
    container.add_dataset(ShapefileDataset(shapefile))

DatasetCollection.add_shapefile_dataset = add_shapefile_dataset
