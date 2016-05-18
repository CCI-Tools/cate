"""
ECT Utilities
=============

Provides random utility functions.

*Implementation note: this module shall not have any dependencies to higher-level ECT modules.*

"""

def qualified_name_to_object(qualified_name: str, default_module_name='builtins'):
    """
    Convert a fully qualified name into a Python object.
    It is true that ``qualified_name_to_object(object_to_qualified_name(obj)) is obj``.

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

    :param value: some Python object
    :param fail: raise ``ValueError``if name cannot be derived.
    :param default_module_name: if this is the *value*'s module name, no module name will be returned.
    :return: fully qualified name if it can be derived, otherwise``None`` if *fail* is ``False``.
    :raise ValueError: if *fail* is``True`` and the name cannot be derived.
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
