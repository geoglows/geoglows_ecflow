"""
Base class for classes implementing Visitor pattern

Derived classes will define handlers for visitables.
The handler signature must be:

   .visit_[VisitableClassName](self, visitable)

Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

class Visitor(object):

    def visit(self, visitable):
        visitable_class_hierarchy = [t.__name__ for t in type(visitable).mro()]
        for visitable_class_name in visitable_class_hierarchy:
            handler_name = 'visit_' + visitable_class_name
            handler = getattr(self, handler_name, None)
            if handler is None:
                continue
            return handler(visitable)
        return self.generic_visit(visitable)

    def generic_visit(self, visitable):
        raise RuntimeError(
            '{} of type {} is not supported'.format(
                repr(visitable), type(visitable)))
