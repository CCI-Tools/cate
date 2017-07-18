=========
Workflows
=========

Overview
========

Workflows are a fundamental concept in Cate. A workflow is an acyclic processing graph made up from workflow steps
of various types:

* Steps that invoke *operations* - (Python) functions with additional meta-information;
* Steps that invoke Python expressions or scripts;
* Steps that invoke another workflow;
* Steps that invoke an external executable.

At the time of writing, Cate has support for the first type, operation steps (CLI and GUI), and limited support
for steps that invoke another workflows (CLI only). The other step types will be added in future releases.

The idea of a workflow is to combine multiple processing steps and treat them as a new operation
without having the need to program. Workflows can have zero, one or more *inputs* of arbitrary data type.
The *outputs* of one step can be used as the input for any other step.
They can also be used as the output of the workflow itself.

Cate externally represents workflows in form of JSON-formatted text files. Support for YAML will be added later.

An internal workflow JSON file is also the most important part of a Cate *workspace* directory as explained further
in section :ref:`about_workspaces`.

Example
=======

This workflow opens a dataset, creates a spatial subset, and writes the result into a new NetCDF file.
The parameters to this workflow are:

* the input file path
* the subset region
* the output file name

.. code-block:: console

    {
      "qualified_name": "subset_netcdf",
      "header": {
        "description": "This workflow creates a spatial subset of a dataset read from a NetCDF file."
      },
      "inputs": {
        "input_file": {
          "data_type": "str",
          "description": "Input NetCDF file path"
        },
        "region": {
          "data_type": "str",
          "description": "Subset region (as Geometry WKT)"
        },
        "output_file": {
          "data_type": "str",
          "description": "Output NetCDF file path"
        },
      },
      "outputs": {
        "return": {
          "source": "subset.return",
          "data_type": "xarray.dataset.Dataset",
          "description": "The spatial subsetted dataset"
        }
      },
      "steps": [
        {
          "id": "read",
          "op": "read_netcdf",
          "inputs": {
            "file": { "source": "subset_netcdf.input_file" }
          }
        },
        {
          "id": "subset",
          "op": "subset_spatial",
          "inputs": {
            "ds": { "source": "read" }
            "region": { "source": "subset_netcdf.region" }
          }
        },
        {
          "id": "write",
          "op": "write_netcdf4",
          "inputs": {
            "obj": { "source": "subset" }
            "file": { "source": "subset_netcdf.output_file" }
          }
        },
      ]
    }


JSON-format
===========

The workflow is represented in JSON format that uses five different keywords on its top level:

* ``qualified_name``
* ``header``
* ``inputs``
* ``outputs``
* ``steps``

The ``qualified_name`` contains a name under which the workflow can be referenced. This is the workflow's operation name.
The ``header`` section contains meta-information about the workflow, for example a description text or a version number.
In the ``input`` section each input to the workflow is listed together with its data type and description.
If a workflow has one or more outputs, an ``output`` section lists the named outputs of a workflow together with
their sources.

The ``step`` section lists the individiual steps of a workflow that are executed sequentially.
The values of the input parameter are taken from the parameters declared in the top-level ``input`` section or
from the output section of another workflow step.


JSON Workflow Schema
====================

