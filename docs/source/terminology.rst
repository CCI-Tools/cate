.. _[RD-9]: http://www.wmo.int/pages/prog/sat/documents/ARCH_strategy-climate-architecture-space.pdf
.. _[RD-10]: http://ceos.org/document_management/Working_Groups/WGClimate/Meetings/WGClimate-6/WGClimate_ECV-Inventory-Questionnaire-Guide_v2-2_Feb2016.pdf

.. attention::
    Cate has become the ESA Climate Toolbox.
    This documentation is being discontinued.
    You can find the documentation on the ESA Climate Toolbox
    `here <http://esa-climate-toolbox.readthedocs.io/>`_.

===========
Terminology
===========

The following table :numref:`terminology` based on `[RD-9]`_ and `[RD-10]`_ lists some of the terms used
in the CCI Toolbox user interfaces and throughout this documentation.

.. list-table:: CCI Toolbox Terminology
   :name: terminology
   :widths: 5 25
   :header-rows: 1
   
   * - Term
     - CCI Toolbox Definition
   * - ECV
     - Umbrella term for geophysical quantity/quantities associated with climate variation and change as well as the
       impact of climate change onto Earth (e.g. cloud properties).
   * - ECV product
     - Subdivision of ECVs in long-term data record of values or fields, covering one or more geophysical quantities
       (e.g. Cloud Water Path).
   * - Geophysical quantity
     - One physical parameter/variable in that constitutes a time series of observations (e.g. Cloud Liquid Water
       Path).
   * - Dataset
     - In-memory representation of data opened from a *data source*. Contains multiple layers of a geophysical quantity or
       multiple geophysical quantities with multiple layers encompassing e.g. information on temporal and spatial
       dimensions and localization or uncertainty information.
   * - Data product
     - Combination of dataset and geophysical quantity incl. uncertainty information (e.g. Cloud Liquid Water Path
       from L3S Modis merged phase1 v1.0 including uncertainty, standard deviation, number of observations, …)
   * - Data store
     - Offers multiple *data sources*.
   * - Data source
     - A concrete source for datasets. Provides the *schema* of datasets and other descriptive meta-information
       about a dataset such as it geo-spatial coverage. Used to open datasets.
   * - Schema
     - Describes a dataset's structure, contents and data types.


        
