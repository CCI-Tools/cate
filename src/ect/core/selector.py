import abc


class BoundaryValueBase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def eval(self, variable):
        pass

    def __and__(self, other):
        return BoundaryValueAnd(self, other)


class BoundaryValueAnd(BoundaryValueBase):
    def __init__(self, bv1, bv2):
        self.bv1 = bv1
        self.bv2 = bv2

    def eval(self, variable):
        r1 = self.bv1.eval()
        r2 = self.bv2.eval()
        for k, v in r2.items():
            if not k in r1:
                r1[k] = r2[k]
            else:
                # concat!
                pass


class BoundaryValue(BoundaryValueBase):
    def __init__(self, *names, **coordinates):
        self.names = names
        self.coordinates = coordinates

    def eval(self, variable):
        r = {}
        for k, v in self.coordinates.items():
            if v.callable():
                coord = variable.coords[k]
                r[k] = v(coord)
            else:
                r[k] = v
        return r
