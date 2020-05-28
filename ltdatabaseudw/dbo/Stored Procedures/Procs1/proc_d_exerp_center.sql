CREATE PROC [dbo].[proc_d_exerp_center] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_center)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_center_insert') is not null drop table #p_exerp_center_insert
create table dbo.#p_exerp_center_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_center.p_exerp_center_id,
       p_exerp_center.bk_hash
  from dbo.p_exerp_center
 where p_exerp_center.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_center.dv_batch_id > @max_dv_batch_id
        or p_exerp_center.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_center.bk_hash,
       p_exerp_center.center_id center_id,
       s_exerp_center.name center_name,
       s_exerp_center.county county,
       l_exerp_center.external_id external_id,
       s_exerp_center.latitude latitude,
       s_exerp_center.longitude longitude,
       case when p_exerp_center.bk_hash in('-997', '-998', '-999') then p_exerp_center.bk_hash
        when l_exerp_center.manager_person_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_center.manager_person_id as varchar(4000)),'z#@$k%&P'))),2) end manager_dim_employee_key,
       case when p_exerp_center.bk_hash in('-997', '-998', '-999') then p_exerp_center.bk_hash
           when s_exerp_center.migration_date is null then '-998'
        else convert(varchar, s_exerp_center.migration_date, 112) end migration_dim_date_key,
       case when p_exerp_center.bk_hash in('-997', '-998', '-999') then p_exerp_center.bk_hash
           when s_exerp_center.startup_date is null then '-998'
        else convert(varchar, s_exerp_center.startup_date, 112) end startup_dim_date_key,
       s_exerp_center.time_zone time_zone,
       isnull(h_exerp_center.dv_deleted,0) dv_deleted,
       p_exerp_center.p_exerp_center_id,
       p_exerp_center.dv_batch_id,
       p_exerp_center.dv_load_date_time,
       p_exerp_center.dv_load_end_date_time
  from dbo.h_exerp_center
  join dbo.p_exerp_center
    on h_exerp_center.bk_hash = p_exerp_center.bk_hash
  join #p_exerp_center_insert
    on p_exerp_center.bk_hash = #p_exerp_center_insert.bk_hash
   and p_exerp_center.p_exerp_center_id = #p_exerp_center_insert.p_exerp_center_id
  join dbo.l_exerp_center
    on p_exerp_center.bk_hash = l_exerp_center.bk_hash
   and p_exerp_center.l_exerp_center_id = l_exerp_center.l_exerp_center_id
  join dbo.s_exerp_center
    on p_exerp_center.bk_hash = s_exerp_center.bk_hash
   and p_exerp_center.s_exerp_center_id = s_exerp_center.s_exerp_center_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_center
   where d_exerp_center.bk_hash in (select bk_hash from #p_exerp_center_insert)

  insert dbo.d_exerp_center(
             bk_hash,
             center_id,
             center_name,
             county,
             external_id,
             latitude,
             longitude,
             manager_dim_employee_key,
             migration_dim_date_key,
             startup_dim_date_key,
             time_zone,
             deleted_flag,
             p_exerp_center_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         center_id,
         center_name,
         county,
         external_id,
         latitude,
         longitude,
         manager_dim_employee_key,
         migration_dim_date_key,
         startup_dim_date_key,
         time_zone,
         dv_deleted,
         p_exerp_center_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_center)
--Done!
end
