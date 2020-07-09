from unittest import TestCase

import pandas as pd

from cate.util.time import find_datetime_format
from cate.util.time import get_timestamp_from_string
from cate.util.time import get_timestamps_from_string

class TimeTest(TestCase):

    def test_find_datetime_format(self):
        format, start_index, end_index = find_datetime_format('ftze20140305131415dgs')
        self.assertEqual('%Y%m%d%H%M%S', format)
        self.assertEqual(4, start_index)
        self.assertEqual(18, end_index)

        format, start_index, end_index = find_datetime_format('ftze201403051314dgs')
        self.assertEqual('%Y%m%d%H%M', format)
        self.assertEqual(4, start_index)
        self.assertEqual(16, end_index)

        format, start_index, end_index = find_datetime_format('ft2ze20140307dgs')
        self.assertEqual('%Y%m%d', format)
        self.assertEqual(5, start_index)
        self.assertEqual(13, end_index)

        format, start_index, end_index = find_datetime_format('ft2ze201512dgs')
        self.assertEqual('%Y%m', format)
        self.assertEqual(5, start_index)
        self.assertEqual(11, end_index)

        format, start_index, end_index = find_datetime_format('ft2s6ze2016dgs')
        self.assertEqual('%Y', format)
        self.assertEqual(7, start_index)
        self.assertEqual(11, end_index)

    def test_get_timestamp_from_string(self):
        timestamp = get_timestamp_from_string('ftze20140305131415dgs')
        self.assertEqual(pd.Timestamp('2014-03-05T13:14:15'), timestamp)

    def test_get_timestamps_from_string(self):
        timestamp_1, timestamp_2 = \
            get_timestamps_from_string('20020401-20020406-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc')
        self.assertEqual(pd.Timestamp('2002-04-01'), timestamp_1)
        self.assertEqual(pd.Timestamp('2002-04-06'), timestamp_2)

        timestamp_1, timestamp_2 = \
            get_timestamps_from_string('20020401-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc')
        self.assertEqual(pd.Timestamp('2002-04-01'), timestamp_1)
        self.assertIsNone(timestamp_2)
