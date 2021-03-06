﻿CREATE PROC [dbo].[proc_d_exerp_resource] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_resource)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_resource_insert') is not null drop table #p_exerp_resource_insert
create table dbo.#p_exerp_resource_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_resource.p_exerp_resource_id,
       p_exerp_resource.bk_hash
  from dbo.p_exerp_resource
 where p_exerp_resource.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_resource.dv_batch_id > @max_dv_batch_id
        or p_exerp_resource.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_resource.bk_hash,
       p_exerp_resource.resource_id resource_id,
       l_exerp_resource.access_group_id access_group_id,
       s_exerp_resource.comment comment,
       case when p_exerp_resource.bk_hash in('-997', '-998', '-999') then p_exerp_resource.bk_hash
           when l_exerp_resource.center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_resource.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       l_exerp_resource.external_id external_id,
       s_exerp_resource.access_group_name resource_access_group_name,
       s_exerp_resource.name resource_name,
       s_exerp_resource.state resource_state,
       s_exerp_resource.type resource_type,
       case when show_calendar = 1 then 'Y' else 'N' end show_calendar,
       isnull(h_exerp_resource.dv_deleted,0) dv_deleted,
       p_exerp_resource.p_exerp_resource_id,
       p_exerp_resource.dv_batch_id,
       p_exerp_resource.dv_load_date_time,
       p_exerp_resource.dv_load_end_date_time
  from dbo.h_exerp_resource
  join dbo.p_exerp_resource
    on h_exerp_resource.bk_hash = p_exerp_resource.bk_hash
  join #p_exerp_resource_insert
    on p_exerp_resource.bk_hash = #p_exerp_resource_insert.bk_hash
   and p_exerp_resource.p_exerp_resource_id = #p_exerp_resource_insert.p_exerp_resource_id
  join dbo.l_exerp_resource
    on p_exerp_resource.bk_hash = l_exerp_resource.bk_hash
   and p_exerp_resource.l_exerp_resource_id = l_exerp_resource.l_exerp_resource_id
  join dbo.s_exerp_resource
    on p_exerp_resource.bk_hash = s_exerp_resource.bk_hash
   and p_exerp_resource.s_exerp_resource_id = s_exerp_resource.s_exerp_resource_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_resource
   where d_exerp_resource.bk_hash in (select bk_hash from #p_exerp_resource_insert)

  insert dbo.d_exerp_resource(
             bk_hash,
             resource_id,
             access_group_id,
             comment,
             d_exerp_center_bk_hash,
             external_id,
             resource_access_group_name,
             resource_name,
             resource_state,
             resource_type,
             show_calendar,
             deleted_flag,
             p_exerp_resource_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         resource_id,
         access_group_id,
         comment,
         d_exerp_center_bk_hash,
         external_id,
         resource_access_group_name,
         resource_name,
         resource_state,
         resource_type,
         show_calendar,
         dv_deleted,
         p_exerp_resource_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_resource)
--Done!
end
