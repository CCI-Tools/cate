from abc import ABCMeta, abstractmethod


class DatasetOperations(metaclass=ABCMeta):
    """
    A collection of generic operations that can act both on vector (?) and gridded raster data (xarray.Dataset).
    """

    @abstractmethod
    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Return a subset of the dataset.

        :param spatial_roi: The spatial region of interest
        :param temporal_roi: : The temporal region of interest
        :return: subset of the dataset as a dataset of type ``Dataset``.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes data access.
        """
        pass


class Dataset(DatasetOperations, metaclass=ABCMeta):
    """
    An abstract base class representing a generic dataset adapter that can apply all
    **DatasetOperations** to a wrapped dataset of any type.
    """

    def __init__(self, dataset: object):
        self._dataset = dataset

    @property
    def dataset(self):
        return self._dataset


class DatasetCollection(DatasetOperations):
    """
    A data container contains datasets of various types (vector and raster data)
    and implements a set of common operations on these datasets.
    """

    def __init__(self):
        self._datasets = []

    @property
    def datasets(self):
        return [ds.dataset for ds in self._datasets]

    def add_dataset(self, dataset):
        self._datasets.append(dataset)

    def remove_dataset(self, dataset):
        for ds in self._datasets:
            if ds.dataset is dataset:
                self._datasets.remove(ds)
                return ds
        self._datasets.remove(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        dsc = DatasetCollection()
        for dataset in self._datasets:
            dsc.add_dataset(dataset.subset(spatial_roi=spatial_roi, temporal_roi=temporal_roi))
        return dsc

    def close(self):
        for dataset in self._datasets:
            dataset.close()
