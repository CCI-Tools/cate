===========
Desktop GUI
===========

*Applies to Cate Desktop, version 0.8.0*

Overview
========

*Cate Desktop* is a desktop application and is intended to serve as the primary graphical user interface (GUI)
for the CCI Toolbox.

It provides all the Cate CLI and almost all Cate Python API functionality through a interactive and user friendly
interface and adds some unique imaging and visual data analysis features.

The basic idea of Cate Desktop is to allow access all remote CCI data sources and calling all Cate operations
through a consistent interface. The results of opening a data source or applying an operations is usually
an in-memory dataset representation - this is what Cate calls a *resource*. Usually, a resource refers to
a (NetCDF/CF) dataset comprising one or more geo-physcial variables, but a resource can virtually be of any (Python)
data type.

The initial layout of Cate Desktop, when run for the first time, reflects what just has been described.

.. _gui_initial:

.. figure:: _static/figures/user_manual/gui_initial.png
   :scale: 100 %
   :align: center

   Cate Desktop initial layout

The user interface basically comprises *panels*, *views*, and a *menu bar*.

------
Panels
------

The screenshot in :numref:`_gui_initial` shows four active *panels*:

1. On the upper left, the **DATA SOURCES** panel to browse, download and open both local and remote data sources,
   including data from ESA CCI Open Data Portal;
2. On the lower left, the **OPERATIONS** panel to browse and apply available operations;
3. On the upper right, the **WORKSPACE** panel to browser and select available resources and workflow steps resulting
   from opening data sources and applying operations;
4. On the lower right, the **VARIABLES** panel to browse and select the geo-physical variables contained in the
   selected resource.

Other panels are initially hidden. They are

* On the upper left, the **LAYERS** panel, to manage the imagery layers displayed on the active *World view*;
* On the upper left, the **PLACEMARKS** panel, to manage user placemarks, which may be used as input to
   various operastions, e.g. to create time series plots;
* On the lower right, the **VIEWS** panel, to display and edit properties of the currently active view. It also allows
  for creating new *World views*;
* On the lower right, the **TASKS** panel, to list and possibly cancel running background tasks.


-----
Views
-----

The central area is occupied by *views* that can be arranged in rows and columns. Cate currently offers three view
types:

* The **World view**, displaying imagery data originaling from data variables and placemarks on either a
  3D globe or a 2D map;
* The **table view**, displaying tabular resource and variable data in a table;
* The **figure view**, displaying plots  from special figure resources resulting from the various plotting operations.

There may be multiple views stacked in a row of tabs, where each tab represents a view. One view within a tab row
is selected and visible. The selected view can be split horizontally or vertically by dedicated icon buttons on the
right of the tab row header. A split view can be stacked again by the drop down menu (...) on the right-most position
of the the row tab header.

There is always a single *active view* indicated by the blueish view header text. To activate a view,
click its header text. The active view provides a context for various commands, for example all interactions with
the **LAYERS** and **VIEW** panels are associated with the active view.

Initially, a single World view is opened and active.

--------
Menu Bar
--------

Cate's menu currently comprises the **File**, **View**, and **Help** menus.

The **File** menu comprises *Workspace*-related commands and allows setting **Preferences*:

.. _gui_file_menu:

.. figure:: _static/figures/user_manual/gui_file_menu.png
   :scale: 100 %
   :align: center

   Cate Desktop's File menu (Windows 10)


---------
Reference
---------


DATA SOURCES Panel
------------------


OPERATIONS Panel
----------------

The term *operations* as used in the Cate context includes
functions that

* read datasets from files
* manipulate these dataset
* plot datasets
* write datasets to files


WORKSPACE Panel
---------------


VARIABLES Panel
---------------


Preferences Dialog
------------------

