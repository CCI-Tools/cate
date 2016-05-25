RST
===

``ect-core/doc`` is the ECT documentation folder. Documentation is build from RST files in folder ``source`` using the *Sphinx* tool.
``index.rst`` is the main documentation page which pulls other RST files into the documentation by referring to them.

To install Sphinx run:

     $ conda install Sphinx numpydoc

To build the ECT documentation run:

     $ cd ect-core/doc
     $ make html

or to force regeneration of the documentation, run:

     $ cd ect-core
     $ sphinx-build -E -a -b html doc/source doc/build/html

Then find the HTML documentation in ``ect-core/doc/build/html``.

More info:
* Sphinx Tutorial: http://sphinx-doc.org/tutorial.html
* RST Primer: http://sphinx-doc.org/rest.html#rst-primer
* RST Quickref: http://docutils.sourceforge.net/docs/user/rst/quickref.html

UML
===

``ect-core/doc/source/uml`` contains *PlantUML* (https://sourceforge.net/projects/plantuml/) diagrams.
The PlantUML executable is ``ect-core/doc/plantuml.jar``.
Note there is a very good plugin for editing/rendering PlantUML diagrams in PyCharm and IDEA!

