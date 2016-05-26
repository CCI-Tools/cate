"""
Module Description
==================

Provides random utility functions.

*Implementation note: this module shall not have any dependencies to higher-level ECT modules.*

Module Reference
================
"""
from collections import OrderedDict


class Namespace:
    """
    A dictionary-like object that has dynamic attributes.

    Instances of the ``Namespace`` class have some similarities with JavaScript objects; you can use string keys
    to create new attributes and use a string key as an attribute name later. At the same time, you can determine the
    length of the object and use integer indices as well as slices to access values. A ``Namespace`` remembers
    the order of attributes added by utilizing a ``collections.OrderedDict``.

    Constraints and properties of the ``Namespace`` object:

    * The ``Namespace`` class does not defines any methods on its own in order to avoid naming clashes with added keys.
    * All keys must be string that are valid Python names. Values may be of any type.
    * The order of attributes added is preserved.

    Examples:

    >>> ns = Namespace()
    >>> ns['a'] = 1
    >>> ns['z'] = 2
    >>> len(ns.a)
    2
    >>> ns.a
    1
    >>> ns['z']
    2
    >>> ns[0]
    1
    >>> ns[:]
    [1, 2]
    >>> ns(obj)
    [('a', 1), ('z', 2)]
    >>> ns = Namespace([('a', 1), ('z', 2)])
    >>> list(ns)
    [('a', 1), ('z', 2)]

    :param items: sequence of (attribute-name, attribute-value) pairs
    """

    def __init__(self, items=list()):
        attributes = OrderedDict()
        for name, value in items:
            attributes[name] = value
        object.__setattr__(self, '_attributes', attributes)

    def __contains__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        return key in attributes

    def __len__(self):
        attributes = object.__getattribute__(self, '_attributes')
        return len(attributes)

    def __iter__(self):
        attributes = object.__getattribute__(self, '_attributes')
        return iter(attributes.items())

    def __setitem__(self, key, value):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int):
            key = list(attributes.keys())[key]
        attributes[key] = value

    def __getitem__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int) or isinstance(key, slice):
            return list(attributes.values())[key]
        return attributes[key]

    def __delitem__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int) or isinstance(key, slice):
            key = tuple(attributes.keys())[key]
        del attributes[key]

    def __setattr__(self, name, value):
        attributes = object.__getattribute__(self, '_attributes')
        attributes[name] = value

    def __getattr__(self, name):
        attributes = object.__getattribute__(self, '_attributes')
        if name in attributes:
            return attributes[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __delattr__(self, name):
        attributes = object.__getattribute__(self, '_attributes')
        if name in attributes:
            del attributes[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        attributes = object.__getattribute__(self, '_attributes')
        if len(attributes) == 0:
            return 'Namespace()'
        return 'Namespace(%s)' % repr(list(attributes.items()))


def extend(target_class, property_name=None, property_doc=None):
    """
    Return a class decorator for classes that will become extensions to the given *target_class*.
    The *target_class* will be extended by a new property with the given *name* and the given *doc*.
    The new property will return an instance of the decorated extension class. The property value will be lazily
    created by calling the extension class' ``__init__`` method with the the *target_class* instance
    as only argument.

    Example:

    Let ``Model`` be an existing API class. Now another module wishes to extend the ``Model`` class by additional
    methods. This could be done by inheritance, but this will cause severe compatibility issues once the
    ``Model`` class evolves and break the composition-over-inheritance design principle. In addition,
    instantiation of the derived class must be performed explicitly. Instead, we want all ``Model`` instances to
    automatically include our new methods. Here is the code:::

        @extend(Model, 'my_ext')
        class MyModelExt:
            '''My Model extension''''

            def __init__(self, model):
                self.model = model

            def some_new_method(self, x):
                self.model.some_old_method()
                # ...

        # Model API users can now use the API extension without explicitly instantiating MyModelExt:
        model = Model()
        model.my_ext.some_new_method()

    :param target_class: A target class or sequence of target classes that will be extended.
    :param property_name: The name of the new property in the target class.
                          If ``None``, a name will be derived from the *extension_class*.
    :param property_doc: The docstring of the new property in the target class.
                         If ``None``, the doc-string will be taken from the *extension_class*, if any.
    :return: A decorator.
    """

    def decorator(extension_class):
        return _add_extension(target_class, extension_class, property_name=property_name, property_doc=property_doc)

    return decorator


def _add_extension(target_class, extension_class, property_name=None, property_doc=None):
    """
    Add an "extension" property with *property_name* to the *target_class*. The property will return an
    instance of *extension_class* whose ``__init__`` method will be called with the the *target_class*
    instance as only argument.

    Use this function to dynamically add extensions to existing classes in order to avoid inheritance.
    This function should be used through its decorator function :py:func:`extend`.

    :param target_class: A target class or sequence of target classes that will be extended.
    :param extension_class: The class that implements the extension.
    :param property_name: The name of the new property in the target class.
                          If ``None``, a name will be derived from the *extension_class*.
    :param property_doc: The docstring of the new property in the target class.
                         If ``None``, the doc-string will be taken from the *extension_class*, if any.
    :return: The *extension_class*.
    """

    if not property_name:
        # generate a property name from extension class name
        property_name = []
        last_was_lower = False
        for c in extension_class.__name__:
            if last_was_lower and c.isupper():
                property_name.append('_')
            property_name.append(c.lower())
            last_was_lower = c.islower()
        property_name = ''.join(property_name)
    attribute_name = '_' + property_name

    # define a property getter that lazily creates the extension instance
    def _lazy_extension_getter(self):
        if hasattr(self, attribute_name):
            extension = getattr(self, attribute_name)
        else:
            extension = extension_class(self)
            setattr(self, attribute_name, extension)
        return extension

    # derive docstring for property
    doc = property_doc if property_doc else getattr(extension_class, '__doc__', None)

    # inject new property into all target classes
    try:
        iterator = iter(target_class)
    except TypeError:
        iterator = iter([target_class])
    for cls in iterator:
        if hasattr(cls, property_name):
            raise ValueError("an attribute with name '%s' already exists in %s", property_name, cls)
        setattr(cls, property_name, property(fget=_lazy_extension_getter, doc=doc))

    return extension_class


def qualified_name_to_object(qualified_name: str, default_module_name='builtins'):
    """
    Convert a fully qualified name into a Python object.
    It is true that ``qualified_name_to_object(object_to_qualified_name(obj)) is obj``.

    >>> qualified_name_to_object('unittest.TestCase')
    <class 'unittest.case.TestCase'>

    See also :py:func:`object_to_qualified_name`.

    :param qualified_name: fully qualified name of the form [<module>'.'{<name>'.'}]<name>
    :param default_module_name: default module name to be used if the name does not contain one
    :return: the Python object
    :raise ImportError: If the module could not be imported
    :raise AttributeError: If the name could not be found
    """
    parts = qualified_name.split('.')

    if len(parts) == 1:
        module_name = default_module_name
    else:
        module_name = parts[0]
        parts = parts[1:]

    value = __import__(module_name)
    for name in parts:
        value = getattr(value, name)
    return value


def object_to_qualified_name(value, fail=False, default_module_name='builtins') -> str:
    """
    Get the fully qualified name of a Python object.
    It is true that ``qualified_name_to_object(object_to_qualified_name(obj)) is obj``.

    >>> from unittest import TestCase
    >>> object_to_qualified_name(TestCase)
    'unittest.case.TestCase'

    See also :py:func:`qualified_name_to_object`.

    :param value: some Python object
    :param fail: raise ``ValueError`` if name cannot be derived.
    :param default_module_name: if this is the *value*'s module name, no module name will be returned.
    :return: fully qualified name if it can be derived, otherwise ``None`` if *fail* is ``False``.
    :raise ValueError: if *fail* is ``True`` and the name cannot be derived.
    """

    module_name = value.__module__ if hasattr(value, '__module__') else None
    if module_name == default_module_name:
        module_name = None

    # Not sure, if '__qualname__' is the better choice - no Pythons docs available
    name = value.__name__ if hasattr(value, '__name__') else None
    if name:
        return module_name + '.' + name if module_name else name

    if fail:
        raise ValueError("missing attribute '__name__'")
    return None
