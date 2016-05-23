"""
Module Description
==================

Provides random utility functions.

*Implementation note: this module shall not have any dependencies to higher-level ECT modules.*

Module Reference
================
"""
from collections import OrderedDict


class Attributes:
    """
    Instances of the ``Attributes`` class have some similarities with JavaScript objects; you can use string keys
    to create new attributes and use a string key as an attribute name later. At the same time, you can determine the
    length of the object and use integer indices as well as slices to access values.

    Constraints and properties of the ``Attributes`` object:

    * The ``Attributes`` class does not defines any methods on its own in order to avoid naming clashes with added keys.
    * All keys must be string that are valid Python names. Values may be of any type.
    * The order of items added is preserved.

    Examples:

    >>> obj = Attributes()
    >>> obj['a'] = 1
    >>> obj['z'] = 2
    >>> len(obj.a)
    2
    >>> obj.a
    1
    >>> obj['z']
    2
    >>> obj[0]
    1
    >>> obj[:]
    [1, 2]
    >>> list(obj)
    [('a', 1), ('z', 2)]
    >>> obj = Attributes([('a', 1), ('z', 2)])
    >>> list(obj)
    [('a', 1), ('z', 2)]

    :param items sequence of (name, value) pairs
    """

    def __init__(self, items=list()):
        the_dict = OrderedDict()
        for name, value in items:
            the_dict[name] = value
        object.__setattr__(self, '_dict', the_dict)

    def __contains__(self, key):
        the_dict = object.__getattribute__(self, '_dict')
        return key in the_dict

    def __len__(self):
        the_dict = object.__getattribute__(self, '_dict')
        return len(the_dict)

    def __iter__(self):
        the_dict = object.__getattribute__(self, '_dict')
        return iter(the_dict.items())

    def __setitem__(self, key, value):
        the_dict = object.__getattribute__(self, '_dict')
        if isinstance(key, int):
            key = list(the_dict.keys())[key]
        the_dict[key] = value

    def __getitem__(self, key):
        the_dict = object.__getattribute__(self, '_dict')
        if isinstance(key, int) or isinstance(key, slice):
            return list(the_dict.values())[key]
        return the_dict[key]

    def __delitem__(self, key):
        the_dict = object.__getattribute__(self, '_dict')
        if isinstance(key, int) or isinstance(key, slice):
            key = tuple(the_dict.keys())[key]
        del the_dict[key]

    def __setattr__(self, name, value):
        the_dict = object.__getattribute__(self, '_dict')
        the_dict[name] = value

    def __getattr__(self, name):
        the_dict = object.__getattribute__(self, '_dict')
        if name in the_dict:
            return the_dict[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __delattr__(self, name):
        the_dict = object.__getattribute__(self, '_dict')
        if name in the_dict:
            del the_dict[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        the_dict = object.__getattribute__(self, '_dict')
        if len(the_dict) == 0:
            return 'Attributes()'
        return 'Attributes(%s)' % repr(list(the_dict.items()))


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
