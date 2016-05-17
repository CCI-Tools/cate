import fiona

from .cdm import Dataset, DatasetCollection


class FionaDataset(Dataset):
    def __init__(self, dataset: xr.Dataset):
        super(FionaDataset, self).__init__(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using xarray Dataset API
        return self

    def close(self):
        self.dataset.close()


def add_fiona_dataset(container: DatasetCollection, fiona_dataset: fiona.Dataset):
    container.add_dataset(FionaDataset(fiona_dataset))
