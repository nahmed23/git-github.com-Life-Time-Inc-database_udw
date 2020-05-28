CREATE PROC [dbo].[proc_d_ec_plan_items] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_plan_items)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_plan_items_insert') is not null drop table #p_ec_plan_items_insert
create table dbo.#p_ec_plan_items_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_plan_items.p_ec_plan_items_id,
       p_ec_plan_items.bk_hash
  from dbo.p_ec_plan_items
 where p_ec_plan_items.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_plan_items.dv_batch_id > @max_dv_batch_id
        or p_ec_plan_items.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_plan_items.bk_hash,
       p_ec_plan_items.bk_hash fact_trainerize_program_plan_item_key,
       p_ec_plan_items.plan_item_id plan_item_id,
       case when s_ec_plan_items.completed = 1  then 'Y' else 'N' end completed_flag,
       case when p_ec_plan_items.bk_hash in ('-997', '-998', '-999') then p_ec_plan_items.bk_hash   
           when s_ec_plan_items.created_date is null then '-998'   
       	 else convert(char(8), s_ec_plan_items.created_date, 112)   end created_dim_date_key,
       case when p_ec_plan_items.bk_hash in ('-997','-998','-999') then p_ec_plan_items.bk_hash
            when l_ec_plan_items.plan_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_plan_items.plan_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_trainerize_plan_key,
       isnull(s_ec_plan_items.description, '') item_description,
       case when p_ec_plan_items.bk_hash in ('-997', '-998', '-999') then p_ec_plan_items.bk_hash     
           when s_ec_plan_items.date is null then '-998'     
       	else convert(char(8), s_ec_plan_items.date, 112)   end item_dim_date_key,
       case when p_ec_plan_items.bk_hash in ('-997', '-998', '-999') then p_ec_plan_items.bk_hash   
            when s_ec_plan_items.date is null then '-998'   
            else '1' + replace(substring(convert(varchar,s_ec_plan_items.date, 114), 1, 5), ':', '') end item_dim_time_key,
       isnull(s_ec_plan_items.name, '') item_name,
       s_ec_plan_items.item_type item_type,
       l_ec_plan_items.source_id source_id,
       s_ec_plan_items.source_type source_type,
       case when p_ec_plan_items.bk_hash in ('-997', '-998', '-999') then p_ec_plan_items.bk_hash   
           when s_ec_plan_items.updated_date is null then '-998'   
       	 else convert(char(8), s_ec_plan_items.updated_date, 112)   end updated_dim_date_key,
       isnull(h_ec_plan_items.dv_deleted,0) dv_deleted,
       p_ec_plan_items.p_ec_plan_items_id,
       p_ec_plan_items.dv_batch_id,
       p_ec_plan_items.dv_load_date_time,
       p_ec_plan_items.dv_load_end_date_time
  from dbo.h_ec_plan_items
  join dbo.p_ec_plan_items
    on h_ec_plan_items.bk_hash = p_ec_plan_items.bk_hash
  join #p_ec_plan_items_insert
    on p_ec_plan_items.bk_hash = #p_ec_plan_items_insert.bk_hash
   and p_ec_plan_items.p_ec_plan_items_id = #p_ec_plan_items_insert.p_ec_plan_items_id
  join dbo.l_ec_plan_items
    on p_ec_plan_items.bk_hash = l_ec_plan_items.bk_hash
   and p_ec_plan_items.l_ec_plan_items_id = l_ec_plan_items.l_ec_plan_items_id
  join dbo.s_ec_plan_items
    on p_ec_plan_items.bk_hash = s_ec_plan_items.bk_hash
   and p_ec_plan_items.s_ec_plan_items_id = s_ec_plan_items.s_ec_plan_items_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_plan_items
   where d_ec_plan_items.bk_hash in (select bk_hash from #p_ec_plan_items_insert)

  insert dbo.d_ec_plan_items(
             bk_hash,
             fact_trainerize_program_plan_item_key,
             plan_item_id,
             completed_flag,
             created_dim_date_key,
             dim_trainerize_plan_key,
             item_description,
             item_dim_date_key,
             item_dim_time_key,
             item_name,
             item_type,
             source_id,
             source_type,
             updated_dim_date_key,
             deleted_flag,
             p_ec_plan_items_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_trainerize_program_plan_item_key,
         plan_item_id,
         completed_flag,
         created_dim_date_key,
         dim_trainerize_plan_key,
         item_description,
         item_dim_date_key,
         item_dim_time_key,
         item_name,
         item_type,
         source_id,
         source_type,
         updated_dim_date_key,
         dv_deleted,
         p_ec_plan_items_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_plan_items)
--Done!
end
