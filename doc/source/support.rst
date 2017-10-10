.. _User Forum: https://groups.google.com/forum/#!forum/cci-tools
.. _Issue Tracker: https://github.com/CCI-Tools/cate/issues

.. _pull requests: https://help.github.com/articles/creating-a-pull-request-from-a-fork/
.. _Cate repository on GitHub: https://github.com/CCI-Tools/cate

.. _ESGF Portal at CEDA: https://esgf-index1.ceda.ac.uk/projects/esgf-ceda/
.. _ODP Datasets and Data Access Issues: https://github.com/CCI-Tools/cate/wiki/Problems-with-ODP-datasets-and-access
.. _ESA CCI Open Data Portal: http://cci.esa.int/
.. _Issue #64: https://github.com/CCI-Tools/cate/issues/64


=======
Support
=======


User Forum
==========

Please post any feedback, support requests and ideas for the future to the Cate `User Forum`_.


Issue Tracking
==============

Bugs, feature requests, suggestions for improvements should be reported in the Cate `Issue Tracker`_.


Contributing
============

We are happy to receive `pull requests`_ from your fork of the `Cate repository on GitHub`_.


Known Issues
============

Data Access
-----------

1. When running Cate / Cate Desktop on Windows and accessing data from the ESA Open Data Portal,
   you may receive a **SSL certificate verify failed** error.
   The workaround is to visit the `ESGF Portal at CEDA`_ web side using Edge, Chrome, or Firefox.
   This will cause your browser to register the website URL in question with your operating system's
   trusted SSL certificates. See also`Issue #64`_.

2. Not all datasets from offered by the `ESA CCI Open Data Portal`_ can be used in Cate.
   Please check the `ODP Datasets and Data Access Issues`_ page to see whether you problem with
   a dataset is known and if there are already fixes / workarounds.


Other
-----

We have collected all other known issues in the Cate `Issue Tracker`_. If you encounter a problem,
please search for it there first.
