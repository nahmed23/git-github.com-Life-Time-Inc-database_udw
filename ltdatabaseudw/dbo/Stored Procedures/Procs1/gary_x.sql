CREATE PROC [gary_x] @dv_batch_id [bigint] AS
begin

if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select @dv_batch_id max_dv_batch_id,
       @dv_batch_id current_dv_batch_id

if object_id('tempdb..#x') is not null drop table #x
create table dbo.#x with(distribution=round_robin, location=user_db, heap) as
select d_fact_spabiz_ticket_item.fact_spabiz_ticket_item_key,
       d_fact_spabiz_ticket_item.ext_price,
       d_fact_spabiz_ticket.check_in_date_time,
       d_dim_spabiz_staff.first_last_name
  from d_fact_spabiz_ticket_item
  left join d_fact_spabiz_ticket
    on d_fact_spabiz_ticket_item.fact_spabiz_ticket_key = d_fact_spabiz_ticket.fact_spabiz_ticket_key
  left join d_dim_spabiz_staff
    on d_fact_spabiz_ticket.dim_spabiz_staff_key = d_dim_spabiz_staff.dim_spabiz_staff_key
  join #dv_batch_id
    on d_fact_spabiz_ticket_item.dv_batch_id = #dv_batch_id.current_dv_batch_id
    or d_fact_spabiz_ticket_item.dv_batch_id >= #dv_batch_id.max_dv_batch_id
-- where d_fact_spabiz_ticket_item.dv_batch_id = 20171114080146
union
select d_fact_spabiz_ticket_item.fact_spabiz_ticket_item_key,
       d_fact_spabiz_ticket_item.ext_price,
       d_fact_spabiz_ticket.check_in_date_time,
       d_dim_spabiz_staff.first_last_name
  from d_fact_spabiz_ticket_item
  left join d_fact_spabiz_ticket
    on d_fact_spabiz_ticket_item.fact_spabiz_ticket_key = d_fact_spabiz_ticket.fact_spabiz_ticket_key
  left join d_dim_spabiz_staff
    on d_fact_spabiz_ticket.dim_spabiz_staff_key = d_dim_spabiz_staff.dim_spabiz_staff_key
  join #dv_batch_id
    on d_fact_spabiz_ticket.dv_batch_id = #dv_batch_id.current_dv_batch_id
    or d_fact_spabiz_ticket.dv_batch_id >= #dv_batch_id.max_dv_batch_id
-- where d_fact_spabiz_ticket.dv_batch_id = 20171114080146
union
select d_fact_spabiz_ticket_item.fact_spabiz_ticket_item_key,
       d_fact_spabiz_ticket_item.ext_price,
       d_fact_spabiz_ticket.check_in_date_time,
       d_dim_spabiz_staff.first_last_name
  from d_fact_spabiz_ticket_item
  left join d_fact_spabiz_ticket
    on d_fact_spabiz_ticket_item.fact_spabiz_ticket_key = d_fact_spabiz_ticket.fact_spabiz_ticket_key
  left join d_dim_spabiz_staff
    on d_fact_spabiz_ticket.dim_spabiz_staff_key = d_dim_spabiz_staff.dim_spabiz_staff_key
  join #dv_batch_id
    on d_dim_spabiz_staff.dv_batch_id = #dv_batch_id.current_dv_batch_id
    or d_dim_spabiz_staff.dv_batch_id >= #dv_batch_id.max_dv_batch_id
-- where d_dim_spabiz_staff.dv_batch_id = 20171114080146
end
