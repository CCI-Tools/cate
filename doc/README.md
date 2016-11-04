RST
===

``cate-core/doc`` is the Cate documentation folder. Documentation is build from RST files in folder ``source`` using the *Sphinx* tool.
``index.rst`` is the main documentation page which pulls other RST files into the documentation by referring to them.

To install Sphinx and required plugins run:

     $ conda install Sphinx sphinx_rtd_theme
     $ conda install -c conda-forge sphinx-argparse
     $ pip install sphinx-autodoc-annotation


We also use PlantUML for UML diagrams, see ``doc/source/uml/*.puml`` files.
There is a Sphinx plugin, *sphinxcontrib-plantuml*, but unfortunately it isn't working at all.
Therefore, UML diagrams have to be build manually: Type

     $ java -jar plantuml.jar -h
     $ java -jar plantuml.jar source/uml/*.puml -o ../_static/uml -tsvg
     $ java -jar plantuml.jar source/uml/*.puml -o ../_static/uml -tpng

To build the Cate documentation run:

     $ cd cate-core/doc
     $ make html

or to force regeneration of the documentation, run:

     $ cd cate-core
     $ sphinx-build -E -a -b html doc/source doc/build/html

Then find the HTML documentation in ``cate-core/doc/build/html``.

More info:
* Sphinx Tutorial: http://sphinx-doc.org/tutorial.html
* RST Primer: http://sphinx-doc.org/rest.html#rst-primer
* RST Quickref: http://docutils.sourceforge.net/docs/user/rst/quickref.html

UML
===

``cate-core/doc/source/uml`` contains *PlantUML* (https://sourceforge.net/projects/plantuml/) diagrams.
The PlantUML executable is ``cate-core/doc/plantuml.jar``.
Note there is a very good plugin for editing/rendering PlantUML diagrams in PyCharm and IDEA!

