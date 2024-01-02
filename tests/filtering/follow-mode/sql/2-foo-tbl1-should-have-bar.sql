-- foo.tbl1 should have bar in desc_text
select
    desc_text
from
    foo.tbl1
where
    desc_text = 'bar';
