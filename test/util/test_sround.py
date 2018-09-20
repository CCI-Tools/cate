from unittest import TestCase

from cate.util.sround import sround, sround_range


class RoundTest(TestCase):

    def test_sround_samples(self):
        test_data = (
            (2.31243821785e-03, 1, False, 2.3e-03),  # 0
            (2.31243821785e-03, 1, True, 2.3e-03),  # 1
            (2.31243821785e-04, 3, False, 2.312e-04),  # 2
            (2.31243821785e-04, 3, True, 2.312e-04),  # 3
            (2.31243821785e-07, 2, False, 2.31e-07),  # 4
            (2.31243821785e-07, 2, True, 2.31e-07),  # 5
            (2.31243821785e+07, 1, False, 2.31243822e+07),  # 6
            (2.31243821785e+07, 1, True, 2.3e+07),  # 7
            (2.31243821785e+07, 0, False, 2.3124382e+07),  # 8
            (2.31243821785e+07, 0, True, 2e+07),  # 9

            (9.58281445483e-03, 1, False, 9.6e-03),  # 10
            (9.58281445483e-03, 1, True, 9.6e-03),  # 11
            (9.58281445483e-04, 3, False, 9.583e-04),  # 12
            (9.58281445483e-04, 3, True, 9.583e-04),  # 13
            (9.58281445483e-07, 2, False, 9.58e-07),  # 14
            (9.58281445483e-07, 2, True, 9.58e-07),  # 15
            (9.58281445483e+07, 1, False, 9.58281445e+07),  # 16
            (9.58281445483e+07, 1, True, 9.6e+07),  # 17
            (9.58281445483e+07, 0, False, 9.5828145e+07),  # 18
            (9.58281445483e+07, 0, True, 1e+08),  # 19
        )

        for i in range(len(test_data)):
            value, ndigits, int_part, expected_result = test_data[i]
            self.assertEquals(sround(value, ndigits=ndigits, int_part=int_part),
                              expected_result, f"at index #{i}")

    def test_sround_has_limits(self):
        self.assertEquals(sround(1.4825723452345623455e-324, ndigits=10), 0.0)
        self.assertEquals(sround(1.4825723452345623455e+319, ndigits=10, int_part=True), float('+inf'))
        self.assertEquals(sround(-1.4825723452345623455e-324, ndigits=10), 0.0)
        self.assertEquals(sround(-1.4825723452345623455e+319, ndigits=10, int_part=True), float('-inf'))

    def test_sround_range(self):
        self.assertEquals(sround_range((-0.000067128731732, 6.362984893743),
                                       ndigits=1),
                          (0.0, 6.4))

        self.assertEquals(sround_range((-0.000067128731732, 6362.984893743),
                                       ndigits=3),
                          (0.0, 6362.985))

        self.assertEquals(sround_range((6361.239852345, 6362.68923),
                                       ndigits=0),
                          (6361.0, 6363.0))
        self.assertEquals(sround_range((6361.239852345, 6362.68923),
                                       ndigits=2),
                          (6361.24, 6362.69))
        self.assertEquals(sround_range((-6362.68923, +6361.239852345),
                                       ndigits=2),
                          (-6362.69, 6361.24))

        self.assertEquals(sround_range((-0.000067128731732, +0.0027635092345),
                                       ndigits=2),
                          (-0.00007,
                           +0.00276))
        self.assertEquals(sround_range((-0.000067128731732, +0.0027635092345),
                                       ndigits=1),
                          (-0.0001,
                           +0.0028))
        self.assertEquals(sround_range((-0.000067128731732, +0.0027635092345),
                                       ndigits=0),
                          (0.0,
                           +0.003))
