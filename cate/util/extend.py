# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


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
