from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.querybathing import querybathing


# python -m unittest discover tests

class TestWindows(TestCase):
    def test_decisive_equailty(self):

        sql = """
        SELECT 
            name,
            ROW_NUMBER() OVER (PARTITION BY firstname ORDER BY salary DESC) AS SEQUENCE
        FROM Test
        """
        pipeline = querybathing(sql)
        ans = pipeline.parse()
        expected = """final_df = Test\\\n.selectExpr("name","ROW_NUMBER() OVER (PARTITION BY firstname ORDER BY salary DESC) AS SEQUENCE")\n\n"""
        self.assertEqual(ans, expected)