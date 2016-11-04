=============
Configuration
=============

CCI Toolbox' configuration file is called ``conf.py`` and is located in the ``~/.cate`` directory, where ``~`` is
the current user's home directory.

Given here is an overview of the possible configuration parameters (currently only one):

:``data_stores_path``:
    Directory where Cate stores information about data stores and also saves local data files synchronized with their
    remote versions. Use the tilde '~' (also on Windows) within the path to point to your home directory.
    This directory can become rather populated once after a while and it is advisable to place it where there exists
    a high transfer rate and sufficient capacity. Ideally, you would let it point to a dedicated solid state disc (SSD).
    The default value for ``data_stores_path`` is the ``~/.cate/data_stores`` directory.

