CREATE PROC [dbo].[proc_lt_bucks_product_category] @batch_id [bigint],@job_start_date_time [varchar](19) AS
begin

if object_id('tempdb..#category_active') is not null drop table #category_active
create table dbo.#category_active with(distribution=round_robin, location=user_db, heap) as
select category_id, 
       category_parent
  from l_lt_bucks_categories
 where l_lt_bucks_categories_id in (select l_lt_bucks_categories_id from p_lt_bucks_categories where dv_load_end_date_time = 'Dec 31, 9999')

if object_id('tempdb..#h') is not null drop table #h
create table dbo.#h with(distribution=round_robin, location=user_db, heap) as
--categories with themselves
select category_id, 
       category_id category_parent
  from #category_active
union
--categories with parents
select category_id, 
       category_parent
  from #category_active
 where category_parent is not null
   and category_parent <> 0
union
--categories with grandparents
select l1.category_id, 
       l2.category_parent parent_category_id
  from #category_active l1
  join #category_active l2 on l1.category_parent = l2.category_id
 where l2.category_parent is not null
   and l2.category_parent <> 0
union
--categories with great grandparents
select l1.category_id, 
       l3.category_parent parent_category_id
  from #category_active l1
  join #category_active l2 on l1.category_parent = l2.category_id
  join #category_active l3 on l2.category_parent = l3.category_id
 where l3.category_parent is not null
   and l3.category_parent <> 0
union
--categories with great great grandparents
select l1.category_id, 
       l4.category_parent parent_category_id
  from #category_active l1
  join #category_active l2 on l1.category_parent = l2.category_id
  join #category_active l3 on l2.category_parent = l3.category_id
  join #category_active l4 on l3.category_parent = l4.category_id
 where l4.category_parent is not null
   and l4.category_parent <> 0

if object_id('tempdb..#i') is not null drop table #i
create table dbo.#i with(distribution=round_robin, location=user_db, heap) as
select l.citem_id,
       l.citem_product, 
       #h.category_parent,
       convert(datetime,@job_start_date_time,120) dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       @batch_id dv_batch_id,
       5 dv_r_load_source_id,
       row_number() over (order by l.citem_product, #h.category_parent, l.citem_id) r
  from l_lt_bucks_category_items l
  join #h on l.citem_category = #h.category_id
 where l.l_lt_bucks_category_items_id in (select l_lt_bucks_category_items_id from p_lt_bucks_category_items where dv_load_end_date_time = 'Dec 31, 9999')
 group by l.citem_product, #h.category_parent, l.citem_id

truncate table b_lt_bucks_product_category

insert into b_lt_bucks_product_category (
       b_lt_bucks_product_category_id,
       citem_id,
       product_id,
       category_id,
       dv_inserted_date_time,
       dv_insert_user,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id)
select r,
       citem_id,
       citem_product, 
       category_parent,
       getdate(),
       suser_sname(),
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id
  from #i
  
drop table #category_active
drop table #h

end
