=============
Configuration
=============

In order to work, the CCI Toolbox currently requires to store all ECV datasets you would like to work with
on your local computer.

Currently, there is only a single configuration setting which you can use to determine the location of the locally
cached data from ESA's remote CCI FTP server. By default, it is ``~/.ect/data_stores/esa_cci_portal_ftp``.
(Windows users: ``~`` evaluates to your user home directory).

To overwrite the default, set the environment variable ``ECT_DATA_ROOT`` to a directory of your choice.

The data cache directory can become rather populated with data after a while and it is advisable to place
it where there exists a high transfer rate and sufficient capacity. Ideally, you would point ``ECT_DATA_ROOT`` to a
dedicated solid state disc.

