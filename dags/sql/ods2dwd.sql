insert into dwd_cps_order(
    o_id, item_id, item_title, order_create_time
)
select
    o_id
    ,item_id
    ,item_title
    ,order_create_time
from ods_cps_order tmp
on duplicate key update
    item_id = tmp.item_id
    ,item_title = tmp.item_title
    ,order_create_time = tmp.order_create_time;