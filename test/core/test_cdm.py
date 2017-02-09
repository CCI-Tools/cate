import json
from unittest import TestCase

from cate.core.cdm import Schema


class SchemaTest(TestCase):
    @staticmethod
    def _test_schema() -> Schema:
        return Schema('test',
                      dimensions=[Schema.Dimension('lon', length=720),
                                  Schema.Dimension('lat', length=360),
                                  Schema.Dimension('time', length=12)],
                      variables=[Schema.Variable('SST', float, dimension_names=['lon', 'lat', 'time']),
                                 Schema.Variable('qc_flags', int, dimension_names=['lon', 'lat', 'time'])],
                      attributes=[Schema.Attribute('title', str, 'Sea Surface Temperature')])

    def test_rank_and_dim(self):
        schema = self._test_schema()

        self.assertEqual(schema.dimension(2).name, 'time')
        self.assertEqual(schema.dimension('lat').name, 'lat')

        sst = schema.variables[0]
        self.assertEqual(sst.rank, 3)
        self.assertEqual(sst.dimension(schema, 0).name, 'lon')
        self.assertEqual(sst.dimension(schema, 1).name, 'lat')
        self.assertEqual(sst.dimension(schema, 2).name, 'time')

    def test_to_and_from_json(self):
        schema_1 = self._test_schema()

        json_dict_1 = schema_1.to_json_dict()
        json_text_1 = json.dumps(json_dict_1, indent=2)
        # print(json_text_1)

        schema_2 = Schema.from_json_dict(json_dict_1)
        json_dict_2 = schema_2.to_json_dict()
        json_text_2 = json.dumps(json_dict_2, indent=2)
        # print(json_text_2)

        self.maxDiff = None
        self.assertEqual(json_text_1, json_text_2)
