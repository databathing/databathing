from databathing import querybathing

query = "select * from test limit 10"
res = querybathing(query).parse()