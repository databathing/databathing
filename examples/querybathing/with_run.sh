dbath qryparse -q """
with step1 as (
    select firstname, id from df
), step2 as (
    select gender, salary, id from df
), step3 as (
    select 
        s1.id, s1.firstname, s2.gender, s2.salary
    from step1 as s1
    inner join step2 as s2
    on s1.id = s2.id
)
select
    *,
    RANK() OVER (PARTITION BY id ORDER BY salary DESC) AS seq
from step3
"""