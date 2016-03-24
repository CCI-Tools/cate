'doc' is the ECT documentation folder. Documentation is build from *.rst files using Sphinx.
index.rst is the main documentation page.

To install Sphinx run:

     > pip install Sphinx

To build the ECT documentation run:

     > cd ect-core/doc
     > make html

or to force regeneration of the documentation, run:

     > cd ect-core
     > sphinx-build -E -a -b html doc/source doc/build/html

Then find the HTML documentation in ect-core/doc/build/html

More info:
    * Sphinx Tutorial: http://sphinx-doc.org/tutorial.html
    * RST Primer: http://sphinx-doc.org/rest.html#rst-primer