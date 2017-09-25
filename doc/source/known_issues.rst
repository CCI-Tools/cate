.. _ESGF Portal at CEDA: https://esgf-index1.ceda.ac.uk/projects/esgf-ceda/
.. _ODP Datasets and Data Access Issues: https://github.com/CCI-Tools/cate/wiki/Problems-with-ODP-datasets-and-access
.. _ESA CCI Open Data Portal: http://cci.esa.int/
.. _Issue #64: https://github.com/CCI-Tools/cate/issues/64

============
Known Issues
============

Data Access
===========

1. When running Cate or Cate Desktop on Windows and accessing data from the ESA Open Data Portal,
   you may receive a **SSL certificate verify failed** error.
   The workaround is to visit the `ESGF Portal at CEDA`_ web side using Edge, Chrome, or Firefox.
   This will cause your browser to register the website URL in question with your operating system's
   trusted SSL certificates. See also`Issue #64`_.

2. Not all datasets from offered by the `ESA CCI Open Data Portal`_ can be used in Cate.
   Please check the `ODP Datasets and Data Access Issues`_ page to see whether you problem with
   a dataset is known and if there are already fixes / workarounds.


