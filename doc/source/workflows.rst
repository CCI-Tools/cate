=========
Workflows
=========

A workflow is an acyclic processing graph made up from nodes containing operations.
Similar to an operation a workflow has zero or more (named) inputs and outputs.

Purpose
=======

The idea of a workflow is to combine multiple operations together and treat them as a new operation
without having the need to program. The outputs of one operations can be used as the input for other operations.
They can also be used as the output of the workflow itself.


Example
=======

This workflow opens a dataset, creates a spatial subset and writes the result a a new netcdf file.
The parameters to this workflow are:

* the input file name
* the subset region
* the output file name

.. code-block:: console

{
  "qualified_name": "subset_netcdf",
  "header": {
    "description": "This workflow creates a spatial subset from a netcdf."
  },
  "input": {
    "input_file": {
      "data_type": "string",
      "description": "Input netcdf file"
    },
    "region": {
      "data_type": "string",
      "description": "subset region (WKT)"
    },
    "output_file": {
      "data_type": "string",
      "description": "Output netcdf file"
    },
  },
  "output": {
    "return": {
      "source": "subset.return",
      "data_type": "xr.Dataset",
      "description": "The spatial subsetted dataset"
    }
  },
  "steps": [
    {
      "id": "read",
      "op": "read_netcdf",
      "input": {
        "file": { "source": "subset_netcdf.input_file" }
      }
    },
    {
      "id": "subset",
      "op": "subset_spatial",
      "input": {
        "ds": { "source": "read" }
        "region": { "source": "subset_netcdf.region" }
      }
    },
    {
      "id": "write",
      "op": "write_netcdf4",
      "input": {
        "obj": { "source": "subset" }
        "file": { "source": "subset_netcdf.output_file" }
      }
    },
  ]
}


JSON-format
===========

The workflow is represented in JSON format that on the top level has 5 different keywords:

* qualified_name
* header
* input
* output
* steps

The ``qualified_name`` contains a name under which the workflow can be referenced.
The ``header`` section can contain a descripion about the worklfow.
In the ``input`` section each input to the workflow ist listed together with its data type and description.
If a workflow has an output an ``output`` section list the named outputs of a workflow together with their sources.

The ``step`` sectionliste the individiual steps of a workflow tha are executed sequentially.
The values of the input parameter are taken taken from the parameters decalred in the ``input`` section of the workflow or
from the output of another operation.


JSON Workflow Schema
====================

