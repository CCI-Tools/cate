"""
Description
===========

.. warning:: This module has not yet been implemented.

This module implements the `ESRI Shapefile`_ adapter for the ECT common data model.

.. _ESRI Shapefile: https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf


Components
==========
"""

# import fiona
# import shapefile

from .cdm import DatasetAdapter, DatasetCollection


class ShapefileDatasetAdapter(DatasetAdapter):
    def __init__(self, shapefile):
        super(ShapefileDatasetAdapter, self).__init__(shapefile)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using fiona or pyshp API
        return self

    def close(self):
        # implement me using fiona or pyshp API
        pass

    def filter_dataset(self, filter_: tuple = None):
        # implement me using fiona or pyshp API (e.g. feature attribute filtering)
        pass


def add_shapefile_dataset(container: DatasetCollection, shapefile, name: str = None):
    container.add_dataset(ShapefileDatasetAdapter(shapefile), name=name)


# Monkey-patch DatasetCollection
DatasetCollection.add_shapefile_dataset = add_shapefile_dataset
