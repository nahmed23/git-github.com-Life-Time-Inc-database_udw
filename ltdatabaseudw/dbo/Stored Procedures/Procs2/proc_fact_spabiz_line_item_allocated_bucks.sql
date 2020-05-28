CREATE PROC [dbo].[proc_fact_spabiz_line_item_allocated_bucks] @dv_batch_id [bigint] AS
begin

-- Get all bucks payments by ticket
-- Get all processed tickets for the date range
-- Get all line items
--   Get all ticket items for all tickets for the date range
--     Assign a segment_id and calculated the line_item_amount
--   Get all tax items for all tickets for the date range.  Assign a segment_id
--   Get all tip items for all tickets for the date range.  Assign a segment_id
-- Prepare data for calculating bucks allocated per line item
--   Get all line items associated with bucks payments
--   Rank by bucks allocation sequence
-- Allocate bucks/non_bucks within a ticket using allocation sequence
-- Populate fact table combining all line items with allocated bucks
--   truncate the existing data and create base records
-- Insert the items associated with Bucks payments
-- Insert the items not associated with Bucks payments

set xact_abort on
set nocount on

-- Get all bucks payments by ticket
  if object_id('tempdb..#ticket_bucks_payment') is not null drop table #ticket_bucks_payment
  create table dbo.#ticket_bucks_payment with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select fact_spabiz_ticket_payment.fact_spabiz_ticket_key,
         sum(fact_spabiz_ticket_payment.payment_amount) bucks_payment_amount
    from marketing.v_fact_spabiz_ticket_payment fact_spabiz_ticket_payment
    join marketing.v_dim_spabiz_payment_type dim_spabiz_payment_type
      on fact_spabiz_ticket_payment.dim_spabiz_payment_type_key = dim_spabiz_payment_type.dim_spabiz_payment_type_key
   where dim_spabiz_payment_type.name in ('LTBucks', 'LT Bucks')
   group by fact_spabiz_ticket_payment.fact_spabiz_ticket_key

-- Eventually this code can be incremental so keep this for future use
  declare @start_date datetime = 'jan 1, 1900'
  declare @end_date datetime = 'dec 31, 9999'

-- Get all processed tickets for the date range
  if object_id('tempdb..#ticket_tax_tip') is not null drop table #ticket_tax_tip
  create table dbo.#ticket_tax_tip with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select fact_spabiz_ticket.fact_spabiz_ticket_key,
         fact_spabiz_ticket.tax_total,
         fact_spabiz_ticket.tip_amount
    from marketing.v_fact_spabiz_ticket fact_spabiz_ticket
   where fact_spabiz_ticket.status = 'Processed'
     and (fact_spabiz_ticket.created_date_time >= @start_date and fact_spabiz_ticket.created_date_time <= @end_date
          or fact_spabiz_ticket.edit_date_time >= @start_date and fact_spabiz_ticket.edit_date_time <= @end_date)





-- Get all line items
--   Get all ticket items for all tickets for the date range.  Assign a segment_id
if object_id('tempdb..#ticket_item') is not null drop table #ticket_item
  create table dbo.#ticket_item with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select fact_spabiz_ticket_item.fact_spabiz_ticket_item_key,
         fact_spabiz_ticket_item.fact_spabiz_ticket_key,
         fact_spabiz_ticket_item.retail_price,
         fact_spabiz_ticket_item.quantity,
         fact_spabiz_ticket_item.item_discount_amount,
         fact_spabiz_ticket_item.ticket_total_discount_amount,
         fact_spabiz_ticket_item.dim_spabiz_data_type_key,
         fact_spabiz_ticket_item.dim_spabiz_category_key
    from marketing.v_fact_spabiz_ticket_item fact_spabiz_ticket_item
    join #ticket_tax_tip
      on fact_spabiz_ticket_item.fact_spabiz_ticket_key = #ticket_tax_tip.fact_spabiz_ticket_key

--     Assign a segment_id and calculated the line_item_amount
if object_id('tempdb..#ticket_line_item') is not null drop table #ticket_line_item
  create table dbo.#ticket_line_item with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select #ticket_item.fact_spabiz_ticket_item_key,
         #ticket_item.fact_spabiz_ticket_key,
         convert(int,
                 case when dim_spabiz_data_type.data_type_name = 'Product' then 3
                      when dim_spabiz_data_type.data_type_name = 'Service'
                       and isnull(dim_spabiz_category.category_name, '') not like '%Medi%'
                       and isnull(dim_spabiz_category.category_name, '') not like '%Laser%'
                       and isnull(dim_spabiz_category.category_name, '') not like '%Injection%' then 1
                      when dim_spabiz_data_type.data_type_name = 'Service'
                       and (dim_spabiz_category.category_name like '%Medi%'
                            or dim_spabiz_category.category_name like '%Laser%'
                            or dim_spabiz_category.category_name like '%Injection%') then 2
                      when dim_spabiz_data_type.data_type_name = 'Series' then 6
                      when dim_spabiz_data_type.data_type_name = 'Gift Certificate' then 7
                      when dim_spabiz_data_type.data_type_name = 'SERVICE CHARGE' then 8
                      else 1
                  end) segment_id,
-- Look at ticket_id 94881334 as an example of why this is wrong
--         #ticket_item.retail_price * #ticket_item.quantity
--         - #ticket_item.item_discount_amount * #ticket_item.quantity
--         - #ticket_item.ticket_total_discount_amount line_item_amount
         #ticket_item.retail_price * #ticket_item.quantity
         - #ticket_item.item_discount_amount
         - #ticket_item.ticket_total_discount_amount line_item_amount
    from #ticket_item
    join marketing.v_dim_spabiz_data_type dim_spabiz_data_type
      on #ticket_item.dim_spabiz_data_type_key = dim_spabiz_data_type.dim_spabiz_data_type_key
    join marketing.v_dim_spabiz_category dim_spabiz_category
      on #ticket_item.dim_spabiz_category_key = dim_spabiz_category.dim_spabiz_category_key
--   Get all tax items for all tickets for the date range.  Assign a segment_id
 union all
  select null fact_spabiz_ticket_item_key,
         fact_spabiz_ticket_key,
         5 segment_id,
         tax_total line_item_amount
    from #ticket_tax_tip
   where tax_total > 0
--   Get all tip items for all tickets for the date range.  Assign a segment_id
 union all
  select null fact_spabiz_ticket_item_key,
         fact_spabiz_ticket_key,
         4 segment_id,
         tip_amount line_item_amount
    from #ticket_tax_tip
   where tip_amount > 0

-- Prepare data for calculating bucks allocated per line item
--   Get all line items associated with bucks payments
--   Rank by bucks allocation sequence
  if object_id('tempdb..#ticket_line_item_bucks_ranked') is not null drop table #ticket_line_item_bucks_ranked
  create table dbo.#ticket_line_item_bucks_ranked with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select #ticket_line_item.fact_spabiz_ticket_item_key,
         #ticket_line_item.fact_spabiz_ticket_key,
         #ticket_line_item.segment_id,
         #ticket_line_item.line_item_amount,
         #ticket_bucks_payment.bucks_payment_amount,
         dim_spabiz_segment.bucks_allocation_sequence,
         rank() over (partition by #ticket_line_item.fact_spabiz_ticket_key order by dim_spabiz_segment.bucks_allocation_sequence, #ticket_line_item.fact_spabiz_ticket_item_key) ranking
    from #ticket_bucks_payment
    join #ticket_line_item
      on #ticket_bucks_payment.fact_spabiz_ticket_key = #ticket_line_item.fact_spabiz_ticket_key
    join marketing.v_dim_spabiz_segment dim_spabiz_segment
      on #ticket_line_item.segment_id = dim_spabiz_segment.segment_id
   where #ticket_bucks_payment.bucks_payment_amount > 0

-- Allocate bucks/non_bucks within a ticket using allocation sequence
  if object_id('tempdb..#ticket_line_item_allocated_bucks') is not null drop table #ticket_line_item_allocated_bucks
  create table dbo.#ticket_line_item_allocated_bucks with (location = user_db, distribution = hash(fact_spabiz_ticket_key)) as
  select top 0
         #ticket_line_item_bucks_ranked.fact_spabiz_ticket_item_key,
         #ticket_line_item_bucks_ranked.fact_spabiz_ticket_key,
         #ticket_line_item_bucks_ranked.segment_id,
         #ticket_line_item_bucks_ranked.line_item_amount,
         #ticket_line_item_bucks_ranked.bucks_payment_amount allocated_bucks_payment_amount
    from #ticket_line_item_bucks_ranked

  declare @start int = 1
  declare @end int = (select max(ranking) from #ticket_line_item_bucks_ranked)

  while @start <= @end
  begin
    insert into #ticket_line_item_allocated_bucks
                 (fact_spabiz_ticket_item_key,
                  fact_spabiz_ticket_key,
                  segment_id,
                  line_item_amount,
                  allocated_bucks_payment_amount)
    select #ticket_line_item_bucks_ranked.fact_spabiz_ticket_item_key,
           #ticket_line_item_bucks_ranked.fact_spabiz_ticket_key,
           #ticket_line_item_bucks_ranked.segment_id,
           #ticket_line_item_bucks_ranked.line_item_amount,
           case when (#ticket_line_item_bucks_ranked.bucks_payment_amount - isnull(sum(#ticket_line_item_allocated_bucks.allocated_bucks_payment_amount),0))
                       >= #ticket_line_item_bucks_ranked.line_item_amount
                then round(#ticket_line_item_bucks_ranked.line_item_amount,0)
                else round((#ticket_line_item_bucks_ranked.bucks_payment_amount - isnull(sum(#ticket_line_item_allocated_bucks.allocated_bucks_payment_amount),0)),0)
            end allocated_bucks_payment_amount
      from #ticket_line_item_bucks_ranked
      left join #ticket_line_item_allocated_bucks
        on #ticket_line_item_bucks_ranked.fact_spabiz_ticket_key = #ticket_line_item_allocated_bucks.fact_spabiz_ticket_key
     where #ticket_line_item_bucks_ranked.ranking = @start
     group by #ticket_line_item_bucks_ranked.fact_spabiz_ticket_key,
           #ticket_line_item_bucks_ranked.fact_spabiz_ticket_item_key,
           #ticket_line_item_bucks_ranked.segment_id,
           #ticket_line_item_bucks_ranked.bucks_payment_amount,
           #ticket_line_item_bucks_ranked.line_item_amount

     set @start = @start + 1
end

/*
-- These should match and they do
select count(*) from #ticket_line_item_bucks_ranked
select count(*) from #ticket_line_item_allocated_bucks

-- Why don't the following sum up the same?  This is off by too much for Bucks dollar amount rounding errors.
select sum(round(bucks_payment_amount,0)) from #ticket_line_item_bucks_ranked where ranking = 1
select sum(allocated_bucks_payment_amount) from #ticket_line_item_allocated_bucks
-- Find the differences
select p.fact_spabiz_ticket_key, p.bucks_payment_amount, a.sum_allocated_bucks_payment_amount
  from (select bucks_payment_amount, fact_spabiz_ticket_key from #ticket_line_item_bucks_ranked where ranking = 1) p
  join (select sum(allocated_bucks_payment_amount) sum_allocated_bucks_payment_amount, fact_spabiz_ticket_key from #ticket_line_item_allocated_bucks group by fact_spabiz_ticket_key) a
    on p.fact_spabiz_ticket_key = a.fact_spabiz_ticket_key
 where p.bucks_payment_amount != a.sum_allocated_bucks_payment_amount
 order by a.sum_allocated_bucks_payment_amount - p.bucks_payment_amount
select * from #ticket_line_item_bucks_ranked where fact_spabiz_ticket_key = 'DC5170495763574290186BCCF91CD748'
select * from #ticket_line_item_allocated_bucks where fact_spabiz_ticket_key = 'DC5170495763574290186BCCF91CD748'
select * from marketing.v_fact_spabiz_ticket where fact_spabiz_ticket_key = 'DC5170495763574290186BCCF91CD748'
select * from marketing.v_fact_spabiz_ticket_item where fact_spabiz_ticket_key = 'DC5170495763574290186BCCF91CD748'
-- It would appear the differences are all related to Mississauga not joinging to data_type.  Check this again once data_type is fixed and all data is reloaded.
--    and a small amount of difference is from rounding and that's expected
*/

-- Populate fact table combining all line items with allocated bucks
--   truncate the existing data and create base records
truncate table dbo.fact_spabiz_line_item_allocated_bucks
exec proc_util_create_base_records @table_name = 'fact_spabiz_line_item_allocated_bucks'
update dbo.fact_spabiz_line_item_allocated_bucks
   set fact_spabiz_ticket_item_key = convert(varchar, fact_spabiz_line_item_allocated_bucks_id),
       fact_spabiz_ticket_key = convert(varchar, fact_spabiz_line_item_allocated_bucks_id),
       dim_spabiz_segment_key = convert(varchar, fact_spabiz_line_item_allocated_bucks_id),
       bk_hash = convert(varchar, fact_spabiz_line_item_allocated_bucks_id)
-- select * from marketing.v_fact_spabiz_line_item_allocated_bucks

-- Insert the items associated with Bucks payments
insert dbo.fact_spabiz_line_item_allocated_bucks(
             --fact_spabiz_line_item_allocated_bucks_id,
             allocated_bucks_payment_amount,
             dim_spabiz_segment_key,
             fact_spabiz_ticket_item_key,
             fact_spabiz_ticket_key,
             line_item_amount,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
select --row_number() over(order by (select 1)) fact_spabiz_line_item_allocated_bucks_id,
       #ticket_line_item_allocated_bucks.allocated_bucks_payment_amount,
       dim_spabiz_segment.dim_spabiz_segment_key,
       case when #ticket_line_item_allocated_bucks.fact_spabiz_ticket_item_key is null then '-998'
            else #ticket_line_item_allocated_bucks.fact_spabiz_ticket_item_key
        end fact_spabiz_ticket_item_key,
       #ticket_line_item_allocated_bucks.fact_spabiz_ticket_key,
       #ticket_line_item_allocated_bucks.line_item_amount,
       @dv_batch_id,
       getdate(),
       suser_sname()
  from #ticket_line_item_allocated_bucks
  join marketing.v_dim_spabiz_segment dim_spabiz_segment
    on #ticket_line_item_allocated_bucks.segment_id = dim_spabiz_segment.segment_id

-- Insert the items not associated with Bucks payments
insert dbo.fact_spabiz_line_item_allocated_bucks(
             --fact_spabiz_line_item_allocated_bucks_id,
             allocated_bucks_payment_amount,
             dim_spabiz_segment_key,
             fact_spabiz_ticket_item_key,
             fact_spabiz_ticket_key,
             line_item_amount,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
select --row_number() over(order by (select 1)) + (select max(fact_spabiz_line_item_allocated_bucks_id) from dbo.fact_spabiz_line_item_allocated_bucks) fact_spabiz_line_item_allocated_bucks_id,
       0 allocated_bucks_payment_amount,
       dim_spabiz_segment.dim_spabiz_segment_key,
       case when #ticket_line_item.fact_spabiz_ticket_item_key is null then '-998'
            else #ticket_line_item.fact_spabiz_ticket_item_key
        end fact_spabiz_ticket_item_key,
       #ticket_line_item.fact_spabiz_ticket_key,
       #ticket_line_item.line_item_amount,
       @dv_batch_id,
       getdate(),
       suser_sname()
  from #ticket_line_item
  left join (select distinct fact_spabiz_ticket_key from #ticket_line_item_allocated_bucks) distinct_fact_spabiz_ticket_key
    on #ticket_line_item.fact_spabiz_ticket_key = distinct_fact_spabiz_ticket_key.fact_spabiz_ticket_key
  join marketing.v_dim_spabiz_segment dim_spabiz_segment
    on #ticket_line_item.segment_id = dim_spabiz_segment.segment_id
 where distinct_fact_spabiz_ticket_key.fact_spabiz_ticket_key is null

/*
select count(*) from #ticket_line_item
select count(*) from marketing.v_fact_spabiz_line_item_allocated_bucks
select max(fact_spabiz_line_item_allocated_bucks_id) from marketing.v_fact_spabiz_line_item_allocated_bucks
select fact_spabiz_line_item_allocated_bucks_id from marketing.v_fact_spabiz_line_item_allocated_bucks group by fact_spabiz_line_item_allocated_bucks_id having count(*) > 1
*/
end
