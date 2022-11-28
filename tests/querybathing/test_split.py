from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.querybathing import querybathing


# python -m unittest discover tests

class TestDistinct(TestCase):
    def test_decisive_equailty(self):

        sql = """
            SELECT 
                split(name, ",")[0] as first_name
            FROM df
            """
        pipeline = querybathing(sql)
        ans = pipeline.parse()
        expected = """final_df = df\\\n.selectExpr("SPLIT(name, ',')[0] AS first_name")\n\n"""
        self.assertEqual(ans, expected)