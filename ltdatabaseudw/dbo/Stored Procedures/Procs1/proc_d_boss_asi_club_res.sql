CREATE PROC [dbo].[proc_d_boss_asi_club_res] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_club_res)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_club_res_insert') is not null drop table #p_boss_asi_club_res_insert
create table dbo.#p_boss_asi_club_res_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_club_res.p_boss_asi_club_res_id,
       p_boss_asi_club_res.bk_hash
  from dbo.p_boss_asi_club_res
 where p_boss_asi_club_res.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_club_res.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_club_res.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_club_res.bk_hash,
       l_boss_asi_club_res.club club_id,
       p_boss_asi_club_res.resource_id resource_id,
       s_boss_asi_club_res.capacity capacity,
       case when p_boss_asi_club_res.bk_hash in ('-997','-998','-999')
       then p_boss_asi_club_res.bk_hash 
       when s_boss_asi_club_res.created_at is null then '-998'
       else isnull(convert(varchar, s_boss_asi_club_res.created_at, 112),'-998') end created_dim_date_key,
       case when p_boss_asi_club_res.bk_hash in ('-997', '-998', '-999') then p_boss_asi_club_res.bk_hash
    when l_boss_asi_club_res.resource_type_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_asi_club_res.resource_type_id as smallint) as varchar(500)),'z#@$k%&P'))),2) end d_boss_asi_resource_bk_hash,
       case when isnumeric(l_boss_asi_club_res.empl_id) = 1 then l_boss_asi_club_res.empl_id else null end empl_id,
       l_boss_asi_club_res.employee_id employee_id,
       case when p_boss_asi_club_res.bk_hash in ('-997','-998','-999')
       then p_boss_asi_club_res.bk_hash 
       when s_boss_asi_club_res.inactive_end_date is null then '-998'
       else isnull(convert(varchar, s_boss_asi_club_res.inactive_end_date, 112),'-998') end inactive_end_dim_date_key,
       case when p_boss_asi_club_res.bk_hash in ('-997','-998','-999')
       then p_boss_asi_club_res.bk_hash 
       when s_boss_asi_club_res.inactive_start_date is null then '-998'
       else isnull(convert(varchar, s_boss_asi_club_res.inactive_start_date, 112),'-998') end inactive_start_dim_date_key,
       isnull(s_boss_asi_club_res.resource, '') resource,
       s_boss_asi_club_res.resource_type resource_type,
       s_boss_asi_club_res.status status,
       case when p_boss_asi_club_res.bk_hash in ('-997','-998','-999')
       then p_boss_asi_club_res.bk_hash 
       when s_boss_asi_club_res.updated_at is null then '-998'
       else isnull(convert(varchar, s_boss_asi_club_res.updated_at, 112),'-998') end updated_dim_date_key,
       isnull(s_boss_asi_club_res.web_active,'N') web_active,
       isnull(s_boss_asi_club_res.web_enable,'N') web_enable,
       case when p_boss_asi_club_res.bk_hash in ('-997','-998','-999')
       then p_boss_asi_club_res.bk_hash 
       when s_boss_asi_club_res.web_start_date is null then '-998'
       else isnull(convert(varchar, s_boss_asi_club_res.web_start_date, 112),'-998') end web_start_dim_date_key,
       isnull(h_boss_asi_club_res.dv_deleted,0) dv_deleted,
       p_boss_asi_club_res.p_boss_asi_club_res_id,
       p_boss_asi_club_res.dv_batch_id,
       p_boss_asi_club_res.dv_load_date_time,
       p_boss_asi_club_res.dv_load_end_date_time
  from dbo.h_boss_asi_club_res
  join dbo.p_boss_asi_club_res
    on h_boss_asi_club_res.bk_hash = p_boss_asi_club_res.bk_hash
  join #p_boss_asi_club_res_insert
    on p_boss_asi_club_res.bk_hash = #p_boss_asi_club_res_insert.bk_hash
   and p_boss_asi_club_res.p_boss_asi_club_res_id = #p_boss_asi_club_res_insert.p_boss_asi_club_res_id
  join dbo.l_boss_asi_club_res
    on p_boss_asi_club_res.bk_hash = l_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.l_boss_asi_club_res_id = l_boss_asi_club_res.l_boss_asi_club_res_id
  join dbo.s_boss_asi_club_res
    on p_boss_asi_club_res.bk_hash = s_boss_asi_club_res.bk_hash
   and p_boss_asi_club_res.s_boss_asi_club_res_id = s_boss_asi_club_res.s_boss_asi_club_res_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_club_res
   where d_boss_asi_club_res.bk_hash in (select bk_hash from #p_boss_asi_club_res_insert)

  insert dbo.d_boss_asi_club_res(
             bk_hash,
             club_id,
             resource_id,
             capacity,
             created_dim_date_key,
             d_boss_asi_resource_bk_hash,
             empl_id,
             employee_id,
             inactive_end_dim_date_key,
             inactive_start_dim_date_key,
             resource,
             resource_type,
             status,
             updated_dim_date_key,
             web_active,
             web_enable,
             web_start_dim_date_key,
             deleted_flag,
             p_boss_asi_club_res_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         club_id,
         resource_id,
         capacity,
         created_dim_date_key,
         d_boss_asi_resource_bk_hash,
         empl_id,
         employee_id,
         inactive_end_dim_date_key,
         inactive_start_dim_date_key,
         resource,
         resource_type,
         status,
         updated_dim_date_key,
         web_active,
         web_enable,
         web_start_dim_date_key,
         dv_deleted,
         p_boss_asi_club_res_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_club_res)
--Done!
end
