import xarray as xr

from .cdm import Dataset, DatasetCollection


class XArrayDataset(Dataset):
    def __init__(self, dataset: xr.Dataset):
        super(XArrayDataset, self).__init__(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using xarray Dataset API
        return self

    def close(self):
        self.dataset.close()


def add_xarray_dataset(container: DatasetCollection, xr_dataset: xr.Dataset):
    container.add_dataset(XArrayDataset(xr_dataset))

DatasetCollection.add_xarray_dataset = add_xarray_dataset
