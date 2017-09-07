.. _Description of netCDF file contents: http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#description-of-file-contents
.. _CF Conventions: http://cfconventions.org/
.. _Attribute Convention for Data Discovery: http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery
.. _CF Ancillary Data: http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#ancillary-data
.. _CF Flags: http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#flags
.. _xarray : http://xarray.pydata.org
.. _numpy: http://www.numpy.org
.. _pandas: http://pandas.pydata.org
.. _dask: http://dask.pydata.org
.. _line_profiler: https://github.com/rkern/line_profiler

=====================
Operation Development
=====================

In general, operation development follows the general Cate plugin development
approach. E.g., any valid Python function can be decorated accordingly to
introduce it to the Cate plugin system. However, there are some caveats one
should keep in mind, as well as best practices to follow when developing Cate
operations. This chapters explores these issues.


.. _dg-op-technology:

Operation development technology stack
======================================

To develop operations for cate one should be at least cursory familiar with the
following Python projects:

* xarray_
* numpy_
* pandas_
* dask_

The ``xarray`` package is used the most as ``xarray.Dataset`` is the data model
used to represent raster data throughout Cate. Most operations work on xarray datasets and
produce xarray datasets. For tabular data representation and manipulation Cate
supports ``pandas``. ``Numpy`` is the corner stone of both xarray and pandas and is used
when data is explicitly loaded into memory from an xarray object.

The ``dask`` package provides numpy-like data array abstraction of datasets spanning many
on-disk files or even remote locations. A dask array is the underlying array
object type of xarray datasets spanning over multiple files, which is the case
in the large majority of Cate use cases. It can be beneficial to be accustomed
with how dask works in order to write fast, parallelized operations. Not taking
into account how dask works can result in a heavy performance penalty.


.. _dg-op-registration:

Registration with the Cate plugin system
========================================

Any python function can be registered in the cate operation registry by
decorating it accordingly. As the bare minimum the ``@op`` decorator must be
used. Depending on particular circumstances it may be needed to also use other
decorators, such as ``@op_input``, ``@op_output``, ``@op_return``.

For in-depth information on these decorators and their parameters, please check
detailed design of :ref:`dd-cate-core-op` as well as documentation on
:ref:`op` and :ref:`plugin`.

A minimal Cate operation would then look like the following:

.. code-block:: python

  from cate.core.op import op


  @op()
  def dummy_operation(a, b):
      return a + b

A more involved example using tags to ease operation discovery by the user, as
well as accepting file inputs, inputs consisting of known value sets, as well
as variable inputs tied to a particular dataset would look like the following:

.. code-block:: python

  from cate.core.op import op, op_input
  from cate.core.types import VarName


  @op(tags=['geometric'])
  @op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF', extensions=['nc'])])
  @op_input('set', value_set=['a', 'b', 'c'])
  @op_input('var', value_set_source='ds', data_type=VarName)
  def some_operation(ds: xr.Dataset,
                     file: str,
                     set: str = 'a',
                     var: VarName.TYPE):
      # Do some science here
      return ds

In this example we have denoted input named ``file`` as an input that requires
a file browser on the GUI, as well as inputs ``set`` and ``var`` as inputs that
require a drop-down box on the GUI, as well as what values should be in this
drop down box, or where to find them.

We also use the Cate typing system to let other parts of Cate (GUI, CLI) be
aware of what the type of ``var`` is, as well as to enable streamlined
validation. In light of operation development this is described in more detail
here: :ref:`dg-op-cate-typing-system`.

If the newly created operation is meant to be part of the Cate core operation
suite, it should be possible to import it when Cate is used programmatically.
Hence, it should be put in ``cate/ops`` and imported in ``cate/ops/__init__.py``.

Tags
----

Each operation should have at least one tag. This can be the module name,
``input`` or ``output`` in case of operations in the ``io`` module, as well as
a tag from the following list:

  * ``utility`` for any utility operations
  * ``internal`` for internal operations, they will not be shown in user
    interfaces
  * ``geometric`` for geometric operations
  * ``point`` for operations that operate on single lon/lat points
  * ``spatial`` for predominantly spatial operations
  * ``temporal`` for predominantly temporal operations
  * ``filter`` for operations that filter out things from an input to an output


.. _dg-op-history-information:

History information
===================

Well behaved netCDF filters are expected to add information about themselves to
the ``history`` attribute of a netCDF file. See `Description of netCDF file
contents`_.

Cate facilitates this by automatically adding information about Cate, the
particular operation, it's version and invocation parameters to outputs that
have been marked for history addition by providing the appropriate parameter to
``@op_output`` or ``@op_return`` decorators. Note that version information
must be provided to the ``@op`` decorator as well.

.. code-block:: python

  from cate.core.op import op, op_output


  @op(version='1.0')
  @op_output('name2', add_history=True)
  def my_op_that_saves_history_info(ds1: xr.Dataset, ds2: xr.Dataset):
      # Do some science
      return {'name1': ds1, 'name2': ds2}

Here history information will be added only to the ``name2`` outputs. We could
have added ``add_history=True`` to both outputs. Adding history information to
the only outputs, if this outputs is a dataset, can be achieved by using
``@op_return`` in a similar manner.


.. _dg-op-cate-typing-system:

Cate typing system
==================

Operations must use the Cate typing system to ensure that correct controls are
shown in the GUI for the given inputs. Cate typing system also ensures that
part of input validation can be done 'for free' and is located in the same
place, as well as lets one create operations that mimic polymorphism by
accepting multiple input types.

For example, an operation that accepts both an ``xr.Dataset`` and a
``pd.DataFrame``, as well as takes a polygon could look like this:

.. code-block:: python

  from cate.core.types import DatasetLike, PolygonLike
  from cate.core.op import op, op_input


  @op()
  @op_input('dsf', data_type=DatasetLike)
  @op_input('region', data_type=PolygonLike)
  def my_op_using_advanced_types(dsf: DatasetLike.TYPE, region: PolygonLike.TYPE):
      # Convert inputs to base types (implicit validation)
      ds = DatasetLike.convert(dsf)
      region = PolygonLike.convert(region)

      # Do some science

      return ds

Note that the framework requires that Cate typing system is used both in the
decorator, as well as function signature. Here we have made an operation that
accepts both ``xr.Dataset`` and a ``pd.DataFrame`` and converts it to an
``xr.Dataset`` for the actual calculation. We also have a ``region`` parameter
that can be a ``shapely.geometry.Polygon``, a coordinate string, a WKT string,
a list of coordinate points, as well as a list of lon/lat values. Now the GUI
is also aware that the operation expects a polygon and an appropriate dialog
can be displayed.


.. _dg-op-monitor-usage:

Monitor usage
=============

Operations that can be potentially long running should implement a Cate monitor
that can be used by the CLI and the GUI to track the operation's progress, as
well as to cancel the operation. It can sometimes be hard to determine whether
a particular operation will be long running or not. In that case the rule of
thumb should be to err on the side of implementing a monitor.

For example:

.. code-block:: python

  from cate.core.op import op
  from cate.util.monitor import Monitor

  @op()
  def my_op_with_a_monitor(a: str, monitor: Monitor = Monitor.NONE):
      # Set up the monitor
      with monitor.starting('Monitor Operation', total_work=len(a)):
          for i in a:

              # Do work

              # Update the monitor
              monitor.progress(work=1)

              # If there are resources to clean up (e.g., open file handles)
              # use the following instead:
              try:
                  monitor.progress(work=1)
              except Cancellation as c:
                  # Clean up
                  raise c

      return a

Note that special caution should be taken to ensure the correct step size, such
that the task actually ends when the ``total_work`` is reached. Apart from
progress monitoring it is crucial to implement the possibility to cancel long
running operations and perform the appropriate clean up actions when it is
cancelled.

Operations that delagate the compute intensive work to ``xarray`` have often no possibility to
report progress in a meaningful way nor to handle cancellation in a timely manner. In this case
the ``xarray`` task can be observed:

.. code-block:: python

  from cate.core.op import op
  from cate.util.monitor import Monitor
  import xarray as xr

  @op()
  def my_op_with_a_monitor(da: xr.DataArray, monitor: Monitor = Monitor.NONE) -> xr.DataArray:
      # Set up the monitor
      with monitor.observing('Monitor Operation'):
        return da.mean(dim='time')

See also :ref:`api-monitoring`.


.. _dg-op-relevant-conventions:

Adherence to relevant conventions
=================================

Cate software often makes the assumption that most if not all of climate data
towards which the toolbox is geared adhere to `CF Conventions`_ and the
`Attribute Convention for Data Discovery`_ that both complement each other.

On one hand, an operation may make the assumption that data it receives should
be CF compliant. For example, netCDF variables that are ancillary to other
variables, such as uncertainty information, should be denoted as such. See `CF
Ancillary Data`_.

On the other hand, this implies that special care must be taken to ensure that
an operation doesn't break compatibility with said conventions, as well as
heeds the advice given in these conventions when creating new variables or
datasets.

For example, an operation that adds a mask describing another data variable
should follow `CF Ancillary Data`_ and `CF Flags`_. Such an operation can be
examined in ``cate/ops/outliers.py``.

Also, when an operation modifies spatiotemporal extents and/or resolution of
the dataset, the corresponding global attributes from `Attribute Convention for
Data Discovery`_ should be updated or added. There are dedicated functions in
``cate/ops/normalize.py`` for this purpose.

.. code-block:: python

  from cate.ops.normalize import adjust_spatial_attrs, adjust_temporal_attrs


  @op()
  def dummy_op(ds: xr.Dataset):
      rs = ds.copy()

      # Do some science

      # Adjust global attributes
      rs = adjust_spatial_attrs(rs)
      rs = adjust_temporal_attrs(rs)

      return rs


.. _dg-op-operation-outputs:

Operation outputs
=================

Most operations work on ``xr.Datasets`` and return these as well. However, some
operations may produce information that may be best represented in a tabular
form. In these cases it is a good idea to return such data as a
``pd.DataFrame`` instead of an ``xr.Dataset``. This way it can be represented
better in the GUI, on the CLI, as well as in Jupyter notebooks.

Cate supports returning multiple named outputs as a Python dictionary.

.. code-block:: python

  ...
  @op_output('dataset', data_type=xr.Dataset, description='...')
  @op_output('table', data_type=gpd.GeoDataFrame, description='...')
  @op_output('scalar', data_type=float, description='...')
  def my_op_that_has_named_outputs(...):
    ...
    return {'dataset': ds, 'table': df, 'scalar': x}


.. _dg-op-other-operations:

Using other operations
======================

It is a good idea to use other operations when developing other, more involved
operations. Even for seemingly simple cases there might be corner situations
that have been solved in the other operation. For example, one is encouraged to
use the ``subset_spatial`` operation as opposed to directly selecting a dataset
region using ``xr.sel``. Reason being, the given polygon might cross the
antimeridian, a situation which is already solved in
``cate.ops.subset_spatial``.

Some care must be taken when importing other operations to avoid circular
imports. The correct way to import an existing operation is the following:

.. code-block:: python

  # Directly from subset.py
  from cate.ops.subset import subset_spatial


.. _dg-op-testing:

Testing
=======

All operations should be well tested. The unit tests should be fast and verify
the functionality of the operation, not necessarily validate it. Each module in
``cate/ops/`` should have the coresponding test module in ``test/ops/``. A bare
bones test set up for any operation should be the following:

.. code-block:: python

  from unittest import TestCase

  from cate.core.op import OP_REGISTRY
  from cate.util.misc import object_to_qualified_name

  from cate.ops import dummy_op


  class TestDummyOp(TestCase):
      def test_nominal(self):
          """
          Test nominal execution
          """
          expected = 1
          result = dummy_op()
          self.assertEqual(expected, result)

      def test_error(self):
          """
          Test known error conditions
          """
          with self.assertRaises(ValueError) as err:
              dummy_op(param='will error')

It is absolutely crucial to at least have a nominal test that runs the
operation with expected inputs that asserts if the outputs is what was expected,
the imported operation will automatically be invoked through the operation
registry and this will also work in validating if the decorators have been used
properly.

If an operation implements a monitor, it is a good idea to test if it has been
implemented properly. For example:

.. code-block:: python

    from unittest import TestCase
    from cate.util.monitor import ConsoleMonitor
    from cate.ops import dummy_op


    class TestDummyOp(TestCase):
        def test_monitor(self):
            m = ConsoleMonitor()
            result = dummy_op(monitor=m)
            self.assertEqual(m._percentage, 100)

It is also a good idea to test if the dataset meta information is altered
correctly, if newly created data variables have correct attributes, as well as
if unexpected inputs are handled correctly.


.. _dg-op-optimization:

Optimization
============

Profiling
---------

If the operation seems to be too slow it should first be profiled to explore
the opportunities of potential improvement. The line_profiler_ package might
come in handy here. It can be installed via conda ``conda install
line_profiler`` and then used in a notebook to time individual lines of a given
operation as such:

.. code-block:: python

    import cate.ops as ops
    %load_ext line_profiler
    %lprun -f ops.some_op result = some_op()

A caveat here is that while profiling, the operation being profiled should be
undecorated. Otherwise ``line_profiler`` has trouble finding the source code
to test.

Leveraging xarray and dask
--------------------------

When developing operations it should be kept in mind that every operation can
potentially work on out-of-memory datasets. Hence one should try to leverage
possibilities offered by xarray and dask as much as possible.

For example, an operation producing a statistical quantity of a timeseries for
each lon/lat point of a raster could be naively implemented as such:

.. code-block:: python

    import xarray as xr
    from scipy import tricky_stat

    def some_op(da: xr.DataArray):
        """
        Run tricky_stat on the given data array
        """
        for i in range(0, len(ds.lon)):
            for j in range(0, len(ds.lat)):
                array = da.isel(lat=j, lon=i).values
                res[i, j] = tricky_stat(array)

However, this implementation will yield a heavy performance implication due to 
the fact that our ``xr.DataArray`` is likely distributed among many files,
parts of which will be read on each ``da.isel(lat=j, lon=i).values``
invocation resulting in a large overhead in memory and processing time due to
io operations.

A better approach would be to use arithmetics and ``xarray`` ufuncs directly:

.. code-block:: python

    import xarray as xr

    def some_op(da: xr.DataArray):
        """
        Run tricky_stat on the given data array. Influenced by tricky_stat
        scipy implementation.
        """
        da1 = xr.ufuncs.sqrt(da * MAGIC_CONSTANT)
        tricky_stat = da1.mean(dim='time')
        return tricky_stat

This second operation has a potential of running several orders of magnitude
faster due to minimized amount of io operations, as well as additional
optimizations and parallelization occuring behind the scenes in ``xarray`` and
``dask`` code.


.. _dg-op-docs:

Documentation
=============

Operation docstrings are used to provide help information in all channels where
an operation may be used. It is rendered on the command line when ``cate op
info some_op`` is invoked, it is shown in the appropriate places on the GUI,
invoked by users through Python ``help()``, as well as published as part of
Cate documentation. Hence, it is of utmost importance that the docstring
explains well what a particular operation does, as well as documents all input
parameters. See also :ref:`dg-cc-docstrings`.

For example:

.. code-block:: python

    import xarray as xr
    import pandas as pd

    def doc_op(ds: xr.Dataset, df: pd.DataFrame, magical_const: float):
        """
        This operation carries out a well documented calculation.

        References
        ----------
        'Source <http://www.science.org/documented/calculation>'_

        :param ds: The input dataset used for calculation
        :param df: A dataframe containing auxiliary information
        :param magical_const: Magical constant to use for calculation
        :return: Input dataset with documented calculation applied to it
        """
        # Do some science
        return ds

To make sure generated Cate documentation is updated, don't forget to include
the operation in ``cate/doc/source/api_reference.rst``

If an existing operation name is altered, don't forget to run a search through
Cate documentation source to find the possible places where a documentation
update is needed.


.. _dg-op-development-checklist:

Operation development checklist
===============================

* Is the function registered with the operation registry properly?
* Is the operation set up for import in ``cate/ops/__init__.py``?
* Are operation inputs decorated accordingly? E.g., value sets are provided,
  links between variables and datasets established?
* If one or multiple outputs are ``xr.Dataset``, is history information added
  when appropriate?
* Does the operation use cate typing system so that it can be integrated with
  the GUI nicely? Both in the function signature and decorators?
* Are inputs validated?
* If the operation can take a while, does it use a monitor and can be
  cancelled?
* Is the operation a 'well behaved netCDF filter'?

  * If it adds new variables to the netCDF file, do these follow CF conventions?
  * If the operation has the potential of changing spatiotemporal extents and
    or resolution, does it update the global attributes accordingly?
  * Does the operation drop valuable global or variable attributes when it
    shouldn't?

* Does the operation produce an outputs of an appropriate type?
* Are other operations imported correctly if used?
* Is the operation well tested?

  * Is nominal functionality tested?
  * Is the monitor tested?
  * Are the side effects on attributes and other meta information tested?
  * Are error conditions tested?
  * Do the tests run resonably fast?

* Is the operation properly documented?
* Is the operation proprely tagged?

When a newly created operation coresponds to this checklist well, it can be said with
some certainty that the operation behaves well with respect to the Cate


framework, as well as the wider world.
