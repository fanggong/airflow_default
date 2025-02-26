insert into dws_cps_order
select 
    o_id
    ,item_id
    ,item_title
from dwd_cps_order tmp
on duplicate key update
    item_id = tmp.item_id
    ,item_title = tmp.item_title;