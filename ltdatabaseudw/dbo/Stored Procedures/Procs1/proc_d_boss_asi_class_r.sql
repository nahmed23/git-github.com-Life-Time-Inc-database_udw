CREATE PROC [dbo].[proc_d_boss_asi_class_r] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_class_r)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_class_r_insert') is not null drop table #p_boss_asi_class_r_insert
create table dbo.#p_boss_asi_class_r_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_class_r.p_boss_asi_class_r_id,
       p_boss_asi_class_r.bk_hash
  from dbo.p_boss_asi_class_r
 where p_boss_asi_class_r.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_class_r.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_class_r.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_class_r.bk_hash,
       p_boss_asi_class_r.class_r_dept class_r_dept,
       p_boss_asi_class_r.class_r_class class_r_class,
       l_boss_asi_class_r.class_r_format_id class_r_format_id,
       l_boss_asi_class_r.class_r_interest_id class_r_interest_id,
       s_boss_asi_class_r.class_r_updated_at class_r_updated_at,
       case when p_boss_asi_class_r.bk_hash in ('-997','-998','-999') then p_boss_asi_class_r.bk_hash     
         when l_boss_asi_class_r.class_r_interest_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_class_r.class_r_interest_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_interest_bk_hash,
       case when p_boss_asi_class_r.bk_hash in ('-997','-998','-999') then p_boss_asi_class_r.bk_hash     
         when l_boss_asi_class_r.class_r_format_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_class_r.class_r_format_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_product_format_bk_hash,
       s_boss_asi_class_r.class_r_desc product_line,
       case when p_boss_asi_class_r.bk_hash in('-997', '-998', '-999') then p_boss_asi_class_r.bk_hash
           when s_boss_asi_class_r.class_r_updated_at is null then '-998'
        else convert(varchar, s_boss_asi_class_r.class_r_updated_at, 112)    end updated_dim_date_key,
       case when p_boss_asi_class_r.bk_hash in ('-997','-998','-999') then p_boss_asi_class_r.bk_hash
       when s_boss_asi_class_r.class_r_updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_asi_class_r.class_r_updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_boss_asi_class_r.dv_deleted,0) dv_deleted,
       p_boss_asi_class_r.p_boss_asi_class_r_id,
       p_boss_asi_class_r.dv_batch_id,
       p_boss_asi_class_r.dv_load_date_time,
       p_boss_asi_class_r.dv_load_end_date_time
  from dbo.h_boss_asi_class_r
  join dbo.p_boss_asi_class_r
    on h_boss_asi_class_r.bk_hash = p_boss_asi_class_r.bk_hash
  join #p_boss_asi_class_r_insert
    on p_boss_asi_class_r.bk_hash = #p_boss_asi_class_r_insert.bk_hash
   and p_boss_asi_class_r.p_boss_asi_class_r_id = #p_boss_asi_class_r_insert.p_boss_asi_class_r_id
  join dbo.l_boss_asi_class_r
    on p_boss_asi_class_r.bk_hash = l_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.l_boss_asi_class_r_id = l_boss_asi_class_r.l_boss_asi_class_r_id
  join dbo.s_boss_asi_class_r
    on p_boss_asi_class_r.bk_hash = s_boss_asi_class_r.bk_hash
   and p_boss_asi_class_r.s_boss_asi_class_r_id = s_boss_asi_class_r.s_boss_asi_class_r_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_class_r
   where d_boss_asi_class_r.bk_hash in (select bk_hash from #p_boss_asi_class_r_insert)

  insert dbo.d_boss_asi_class_r(
             bk_hash,
             class_r_dept,
             class_r_class,
             class_r_format_id,
             class_r_interest_id,
             class_r_updated_at,
             d_boss_interest_bk_hash,
             d_boss_product_format_bk_hash,
             product_line,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_boss_asi_class_r_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         class_r_dept,
         class_r_class,
         class_r_format_id,
         class_r_interest_id,
         class_r_updated_at,
         d_boss_interest_bk_hash,
         d_boss_product_format_bk_hash,
         product_line,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_boss_asi_class_r_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_class_r)
--Done!
end
