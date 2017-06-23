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

This workflow opens a dataset and writes a plot into a file.
The plot is done at a given lat/lon location for a given variable.
The parameters to this workflow are:

* the name of the dataset
* the variable name
* the latitude value
* the longitude value
* the filename

.. code-block:: console

{
  "qualified_name": "plot_point",
  "header": {
    "description": "This workflow writes a plot to a file."
  },
  "input": {
    "ds_name": {
      "data_type": "string",
      "description": "Dataset name"
    },
    "var_name": {
      "data_type": "string",
      "description": "Varible name"
    },
    "lat": {
      "data_type": "float",
      "description": "Latitude value"
    },
    "lon": {
      "data_type": "string",
      "description": "Longitude value"
    },
    "file_name": {
      "data_type": "string",
      "description": "Plot filename"
    }
  },
  "steps": [
    {
      "id": "open_op",
      "op": "open_dataset",
      "input": {
        "ds_name": { "source": "plot_point.ds_name" }
      }
    },
    {
      "id": "plot_op",
      "op": "plot",
      "input": {
        "ds": { "source": "open_op" },
        "var": { "source": "plot_point.var_name" },
        "indexers": "TODO",
        "file": { "source": "plot_point.file_name" },
      }
    }
  ]
}


JSON-format
===========



JSON Workflow Schema
====================

