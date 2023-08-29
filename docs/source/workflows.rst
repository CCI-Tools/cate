.. attention::
    Cate has become the ESA Climate Toolbox.
    This documentation is being discontinued.
    You can find the documentation on the ESA Climate Toolbox
    `here <http://esa-climate-toolbox.readthedocs.io/>`_.

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

This workflow reflects Use Case 9 and comprises 4 steps:

1. Making a spatial subset of a (e.g. Aerosol ECV) dataset
2. Making a spatial subset of a (e.g. Cloud ECV) dataset
3. Co-registering the first dataset subset with the second
4. Performing a *Pearson Correlation* on two variables of the co-registered subsets


.. code-block:: console

    {
      "schema_version": 1,
      "qualified_name": "uc09_workflow",
      "header": {
        "description": "Correlation of two variables"
      },
      "inputs": {
        "ds_x": {
          "data_type": "cate.core.types.DatasetLike",
          "description": "The first dataset"
        },
        "ds_y": {
          "data_type": "cate.core.types.DatasetLike",
          "description": "The second dataset"
        },
        "var_x": {
          "data_type": "cate.core.types.VarName",
          "description": "Name of a variable in the first dataset",
          "value_set_source": "ds_x"
        },
        "var_y": {
          "data_type": "cate.core.types.VarName",
          "description": "Name of a variable in the second dataset",
          "value_set_source": "ds_y"
        },
        "region": {
          "data_type": "cate.core.types.PolygonLike",
          "description": "Region given as lon_min,lat_min,lon_max,lat_max or Polygon WKT"
        }
      },
      "outputs": {
        "return": {
          "source": "corr_x_y"
        }
      },
      "steps": [
        {
          "id": "ds_x_sub",
          "op": "cate.ops.subset.subset_spatial",
          "inputs": {
            "ds": "uc09_workflow.ds_x",
            "region": "uc09_workflow.region"
          }
        },
        {
          "id": "ds_y_sub",
          "op": "cate.ops.subset.subset_spatial",
          "inputs": {
            "ds": "uc09_workflow.ds_y",
            "region": "uc09_workflow.region"
          }
        },
        {
          "id": "ds_y_sub_reg",
          "op": "cate.ops.coregistration.coregister",
          "inputs": {
            "ds_master": "ds_x_sub",
            "ds_replica": "ds_y_sub"
          }
        },
        {
          "id": "corr_x_y",
          "op": "cate.ops.correlation.pearson_correlation",
          "inputs": {
            "ds_x": "ds_x_sub",
            "ds_y": "ds_y_sub_reg",
            "var_x": "uc09_workflow.var_x",
            "var_y": "uc09_workflow.var_y"
          }
        }
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

