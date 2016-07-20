"""
Description
===========

.. _xarray: http://xarray.pydata.org/en/stable/
.. _Dask: http://dask.pydata.org/en/latest/
.. _ESRI Shapefile: https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf/docs/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM
.. _Fiona: http://toblerity.org/fiona/
.. _CCI Toolbox URD: https://www.dropbox.com/s/0bhp6uwwk6omj8k/CCITBX-URD-v1.0Rev1.pdf?dl=0

This module provides classes and interfaces used to harmonise the access to and operations on various
types of climate datasets, for example gridded data stored in `netCDF`_ files and vector data originating from
`ESRI Shapefile`_ files.

The goal of the ECT is to reuse existing, and well-known APIs for a given data type to a maximum extend
instead of creating a complex new API. The ECT's common data model is therefore designed as a thin
wrapper around the xarray_ N-D Gridded Datasets Python API that represents nicely netCDF, HDF-5 and OPeNDAP
data types, i.e. Unidata's `Common Data Model`_. For the ESRI Shapefile representation we target at
Fiona_, which reads and writes spatial data files.

The use of xarray_ allows the CCI Toolbox to access and process very large datasets without the need to load them
entirely into memory. This feature is enabled by the internal use of the Dask_ library.

The ECT common data model exposes three important classes:

1. :py:class:`Dataset` - an abstract interface describing the common ECT dataset API
2. :py:class:`DatasetAdapter` - wraps an existing dataset and adapts it to the common ``Dataset`` interface
3. :py:class:`DatasetCollection` - a collection of ``Dataset`` objects and at the same time compatible with the
   common ``Dataset`` interface


Technical Requirements
======================

**Common Data Model**

:Description: A common data model is required that abstracts from underlying (climate) data formats.
:URD References:
    * CCIT-UR-DM0001: a) access, b) ingest, c) display, d) process different kinds and sizes of data
    * CCIT-UR-DM0003: multi-dimensional data
    * CCIT-UR-DM0005: access all ECV data products and metadata via standard user-community interfaces, protocols, and tools
    * CCIT-UR-DM0006: access to and ingestion of ESA CCI datasets
    * CCIT-UR-DM0011: access to and ingestion of non-CCI data
    * CCIT-UR-DM0012: handle different input file formats

----

**Common Set of (Climate) Operations**

:Description: Instances of the common data model are the input for various operations used for climate data
    visualisation, processing, and analysis. Depending on the underlying data format / schema, a given
    operations may not be applicable. The API shall provide the means to chack in advance, if a given operation
    is applicable to a given common data model instance.
:URD-References:
    * CCIT-UR-LM0009 to CCIT-UR-LM0018: Geometric Adjustments/Co-registration.
    * CCIT-UR-LM0019 to CCIT-UR-LM0024: Non-geometric Adjustments.
    * CCIT-UR-LM0025 to CCIT-UR-LM0034: Filtering, Extractions, Definitions, Selections.
    * CCIT-UR-LM0035 to CCIT-UR-LM0043: Statistics and Calculations.
    * CCIT-UR-LM0044: GIS Tools.
    * CCIT-UR-LM0045 to CCIT-UR-LM0050: Evaluation and Quality Control.

----

**Handle large Data Sets**

:Description: A single variable in ECV dataset may contain tens of gigabytes of gridded data.
    The common data model must be able to "handle" data sizes by different means. For example, lazy loading
    of data into memory combined with a programming model that allows for partial processing of data subsets
    within an operation.
:URD References:
    * CCIT-UR-DM0002: handle large datasets
    * CCIT-UR-DM0003: multi-dimensional data
    * CCIT-UR-DM0004: multiple inputs

----

Verification
============

The module's unit-tests are located

* `test/ops/test_resample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_resample_2d.py>`_.
* `test/ops/test_downsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_downsample_2d.py>`_.
* `test/ops/test_upsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_upsample_2d.py>`_.
* `test/ops/test_timeseries.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_timeseries.py>`_.

and may be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=ect/ops/<MODULE>.py`` for extra code coverage
information.


Components
==========
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List

from ect.core.util import object_to_qualified_name, qualified_name_to_object


class Schema:
    """
    .. _Schema for NcML: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/ncml/AnnotatedSchema4.html
    .. _netCDF Java Schema: https://www.unidata.ucar.edu/software/netcdf/java/docs/ucar/netcdf/Schema.html
    .. _GeoJSON: http://geojson.org/geojson-spec.html
    .. _Shapefile: https://en.wikipedia.org/wiki/Shapefile

    Simple data structure description that focuses on the (geophysical) variables provided by some dataset. It is
    mainly modelled after the netCDF CD common data model (see also `Schema for NcML`_, `netCDF Java Schema`_).
    However, this schema intentionally lacks the explicit definition of *groups*, as defined by the netCDF CDM.
    Groups are no more than a physical container of variables which can be easily represented as parent
    path components of names of variables, dimensions, and attributes. E.g. if a variable is named ``data/ndvi`` then
    it is in group ``data``. If an attribute is named ``data/ndvi/originator`` then it is an attribute of variable
    ``ndvi`` which is in the group ``data``.

    This schema allows to represent both raster / gridded data types and GIS data. Raster / gridded data may originate
    from netCDF, HDF, GeoTIFF, or others. GIS-type vector data types may originate
    from a Shapefile_ or GeoJSON_ file. It comprises only three basic data structures:

    * ``Variable`` the primary data provided by a dataset, usually geophysical, climate measurements or computed values.
    * ``Dimension`` provides a description of a dimension used by one or more N-D variables.
    * ``Attribute`` provides meta-information to variables and any groups that occur as path components of an
      attribute name.

    Important note: The name ``Attribute`` used here must not be confused with the "attribute" of a "(simple)
    feature type" as used within the OGC GML/GIS terminology.
    The CCI Toolbox maps attributes of OGC features types to *Variables* to match the terminology used in
    this schema.

    :param dimensions: dimensions in this schema
    :param variables: variables in this schema
    :param attributes: attributes in this schema
    """

    def __init__(self,
                 name: str,
                 lon_name: str = 'lon',
                 lat_name: str = 'lat',
                 time_name: str = 'time',
                 dimensions: List['Schema.Dimension'] = None,
                 variables: List['Schema.Variable'] = None,
                 attributes: List['Schema.Attribute'] = None):

        if not name:
            raise ValueError('name must be given')
        self.name = name
        self.lon_name = lon_name
        self.lat_name = lat_name
        self.time_name = time_name
        self.dimensions = list(dimensions) if dimensions else []
        self.variables =  list(variables) if variables else []
        self.attributes =  list(attributes) if attributes else []

    def dimension(self, index_or_name):
        try:
            return self.dimensions[index_or_name]
        except (IndexError, TypeError):
            for dimension in self.dimensions:
                if dimension.name == index_or_name:
                    return dimension
        return None

    @classmethod
    def from_json_dict(cls, json_dict) -> 'Schema':
        name = json_dict.get('name', None)
        lon_name = json_dict.get('lon_name', 'lon')
        lat_name = json_dict.get('lat_name', 'lat')
        time_name = json_dict.get('time_name', 'time')
        json_dimensions = json_dict.get('dimensions', [])
        json_variables = json_dict.get('variables', [])
        json_attributes = json_dict.get('attributes', [])
        dimensions = []
        for json_dimensions in json_dimensions:
            dimensions.append(Schema.Dimension.from_json_dict(json_dimensions))
        variables = []
        for json_variable in json_variables:
            variables.append(Schema.Variable.from_json_dict(json_variable))
        attributes = []
        for json_attribute in json_attributes:
            attributes.append(Schema.Attribute.from_json_dict(json_attribute))
        return Schema(name, lon_name, lat_name, time_name,
                      dimensions=dimensions,
                      variables=variables,
                      attributes=attributes)

    def to_json_dict(self) -> dict:
        json_dict = OrderedDict()
        json_dict['name'] = self.name
        json_dict['lon_name'] = self.lon_name
        json_dict['lat_name'] = self.lat_name
        json_dict['time_name'] = self.time_name
        json_dict['variables'] = [variable.to_json_dict() for variable in self.variables]
        json_dict['dimensions'] = [dimension.to_json_dict() for dimension in self.dimensions]
        json_dict['attributes'] = [attribute.to_json_dict() for attribute in self.attributes]
        return json_dict

    class Variable:
        """
        Represents a (geophysical) variable of a specified data type and array shape.
        """

        def __init__(self,
                     name: str,
                     data_type: type,
                     dimension_names: List[str] = None,
                     attributes: List['Schema.Attribute'] = None):
            self.name = name
            self.data_type = data_type
            self.dimension_names = list(dimension_names) if dimension_names else []
            self.attributes = list(attributes) if attributes else []

        @property
        def rank(self):
            return len(self.dimension_names)

        def dimension(self, schema: 'Schema', index: int):
            name = self.dimension_names[index]
            return schema.dimension(name)

        @classmethod
        def from_json_dict(cls, json_dict) -> 'Schema.Variable':
            name = json_dict.get('name', None)
            data_type = qualified_name_to_object(json_dict.get('data_type', None))
            dimension_names = json_dict.get('dimension_names', [])
            json_attributes = json_dict.get('attributes', [])
            attributes = []
            for json_attribute in json_attributes:
                attributes.append(Schema.Attribute.from_json_dict(json_attribute))
            return Schema.Variable(name,
                                   data_type,
                                   dimension_names=dimension_names,
                                   attributes=attributes)

        def to_json_dict(self) -> dict:
            json_dict = OrderedDict()
            json_dict['name'] = self.name
            json_dict['data_type'] = object_to_qualified_name(self.data_type)
            json_dict['dimension_names'] = self.dimension_names
            json_dict['attributes'] = [attribute.to_json_dict() for attribute in self.attributes]
            return json_dict

    class Dimension:
        """
        Provides a description of a dimension used by one or more N-D variables.
        """

        def __init__(self, name: str,
                     length=None,
                     attributes: List['Schema.Attribute'] = None):
            self.name = name
            self.attributes = list(attributes) if attributes else []
            if length is not None:
                self.attributes.append(Schema.Attribute('length', int, length))

        @classmethod
        def from_json_dict(cls, json_dict) -> 'Schema.Dimension':
            name = json_dict.get('name', None)
            json_attributes = json_dict.get('attributes', [])
            attributes = []
            for json_attribute in json_attributes:
                attributes.append(Schema.Attribute.from_json_dict(json_attribute))
            return Schema.Dimension(name, attributes=attributes)

        def to_json_dict(self) -> dict:
            json_dict = OrderedDict()
            json_dict['name'] = self.name
            json_dict['attributes'] = [attribute.to_json_dict() for attribute in self.attributes]
            return json_dict

    class Attribute:
        """
        An attribute is a name-value pair of a specified type.
        The main purpose of attributes is to attach meta-information to datasets and variables.
        Values are usually scalars and may remain constant over
        multiple datasets that use the same schema (e.g. missing value, coordinate reference system, originator).
        """

        def __init__(self,
                     name: str,
                     data_type: type = str,
                     value: object = None):
            self.name = name
            self.data_type = data_type
            self.value = value

        @classmethod
        def from_json_dict(cls, json_dict) -> 'Schema.Attribute':
            name = json_dict.get('name', None)
            data_type = qualified_name_to_object(json_dict.get('data_type', None))
            # TODO (nf, 20160627): convert JSON value to Python value
            value = json_dict.get('value', None)
            return Schema.Attribute(name, data_type, value=value)

        def to_json_dict(self) -> dict:
            json_dict = OrderedDict()
            json_dict['name'] = self.name
            json_dict['data_type'] = object_to_qualified_name(self.data_type)
            # TODO (nf, 20160627): convert self.value to JSON value
            json_dict['value'] = self.value
            return json_dict


class Dataset(metaclass=ABCMeta):
    """
    An abstract interface describing the common ECT dataset API.
    """

    @abstractmethod
    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Return a subset of the dataset.

        :param spatial_roi: The spatial region of interest
        :param temporal_roi: : The temporal region of interest
        :return: subset of the dataset as a dataset of type ``Dataset``.
        """

    @abstractmethod
    def filter(self, variable_names:list=None, regex=False, copy:bool=False):
        """
        Filter the dataset, by leaving only desired variables.

        Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
        nature of the data.

        .. _regex: https://docs.python.org/3.5/library/re.html

        :param variable_names: List of regex patterns that identify the variables to keep.
        :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
        :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
        :return: a new, filtered dataset of type :py:class:`Dataset`
        """

    @abstractmethod
    def close(self):
        """
        Closes data access.
        """


class DatasetAdapter(Dataset, metaclass=ABCMeta):
    """
    An abstract base class that wraps an existing dataset or data structure and adapts it to the common
    :py:class:`Dataset` interface.

    :param wrapped_dataset: The wrapped dataset / data structure
    """

    def __init__(self, wrapped_dataset: object):
        self._wrapped_dataset = wrapped_dataset

    @property
    def wrapped_dataset(self):
        """
        :return: The wrapped dataset / data structure
        """
        return self._wrapped_dataset


class DatasetCollection(Dataset):
    """
    A collection of :py:class:`Dataset`-like objects.

    :param datasets: datasets
    :param named_datasets: named datasets
    """

    def __init__(self, *datasets, **named_datasets):
        self._datasets = OrderedDict()
        for dataset in datasets:
            self.add_dataset(dataset)
        for name, dataset in named_datasets.items():
            self.add_dataset(dataset, name=name)

    @property
    def wrapped_datasets(self):
        """
        :return: A sequence of all wrapped datasets / data structures in the order they have been added.
        """
        return [ds.wrapped_dataset for ds in self._datasets.values()]

    @property
    def datasets(self):
        """
        :return: A sequence of all :py:class:`Dataset` objects
                 in this collection in the order they have been added.
        """
        return [ds for ds in self._datasets.values()]

    def add_dataset(self, dataset, name: str = None):
        """
        Add a new dataset to this collection.
        :param dataset: a :py:class:`Dataset`-like object
        :param name: an optional name
        """
        if not name:
            name = 'ds_' + hex(id(dataset))[2:]
        self._datasets[name] = dataset

    def remove_dataset(self, name_or_dataset):
        """
        Remove the given dataset from this collection.

        :param name_or_dataset: The name of the dataset, the dataset, or the wrapped dataset to be removed.
        :return: The :py:class:`Dataset` that has been removed.
        """
        for name, dataset in self._datasets.items():
            if name_or_dataset is dataset.wrapped_dataset \
                    or name_or_dataset is dataset \
                    or name_or_dataset == name:
                del self._datasets[name]
                return dataset
        return None

    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Call the :py:meth:`subset()` method on all datasets and return the result as
        a new dataset collection.

        :param spatial_roi: A spatial region of interest
        :param temporal_roi: A temporal region of interest
        :return: a new dataset collection.
        """
        dsc = DatasetCollection()
        for name, dataset in self._datasets.items():
            dsc.add_dataset(dataset.subset(spatial_roi=spatial_roi, temporal_roi=temporal_roi), name=name)
        return dsc

    def filter(self, variable_names: list = None, regex=False, copy: bool = False):
        """
        Filter the dataset in the collection, by leaving only desired variables. Return a new collection
        that contains the filtered datasets.

        Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
        nature of the data.

        .. _regex: https://docs.python.org/3.5/library/re.html

        :param variable_names: List of variable_names that identify the variables to keep
        :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
        :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
        :return: a new, filtered dataset of type :py:class:`Dataset`
        """

        dsc = DatasetCollection()
        for name, dataset in self._datasets.items():
            dsc.add_dataset(dataset.filter(var_names=variable_names, copy=copy))
        return dsc

    def close(self):
        """
        Closes all datasets.
        """
        for dataset in self.datasets:
            dataset.close()
