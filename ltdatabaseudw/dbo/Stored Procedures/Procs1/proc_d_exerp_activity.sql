CREATE PROC [dbo].[proc_d_exerp_activity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_activity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_activity_insert') is not null drop table #p_exerp_activity_insert
create table dbo.#p_exerp_activity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_activity.p_exerp_activity_id,
       p_exerp_activity.bk_hash
  from dbo.p_exerp_activity
 where p_exerp_activity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_activity.dv_batch_id > @max_dv_batch_id
        or p_exerp_activity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_activity.bk_hash,
       p_exerp_activity.activity_id activity_id,
       l_exerp_activity.access_group_id access_group_id,
       s_exerp_activity.name activity_name,
       s_exerp_activity.state activity_state,
       s_exerp_activity.type activity_type,
       l_exerp_activity_2.age_group_id age_group_id,
       s_exerp_activity.color color,
       s_exerp_activity_1.course_schedule_type course_schedule_type,
       case when p_exerp_activity.bk_hash in ('-997','-998','-999') then p_exerp_activity.bk_hash     
         when l_exerp_activity.activity_group_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_activity.activity_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_activity_group_bk_hash,
       case when p_exerp_activity.bk_hash in ('-997','-998','-999') then p_exerp_activity.bk_hash     
         when l_exerp_activity_2.age_group_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_activity_2.age_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_age_group_bk_hash,
       s_exerp_activity.description description,
       case when p_exerp_activity.bk_hash in ('-997','-998','-999') then p_exerp_activity.bk_hash     
         when l_exerp_activity.external_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ltrim(rtrim(l_exerp_activity.external_id)) as char(15)),'z#@$k%&P'))),2)
        end dim_boss_product_key,
       case when p_exerp_activity.bk_hash in('-997', '-998', '-999') then p_exerp_activity.bk_hash
           when l_exerp_activity_1.time_configuration_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_activity_1.time_configuration_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_exerp_time_configuration_key,
       l_exerp_activity.external_id external_id,
       s_exerp_activity.max_participants max_participants,
       s_exerp_activity.max_waiting_list_participants max_waiting_list_participants,
       l_exerp_activity_1.time_configuration_id time_configuration_id,
       isnull(h_exerp_activity.dv_deleted,0) dv_deleted,
       p_exerp_activity.p_exerp_activity_id,
       p_exerp_activity.dv_batch_id,
       p_exerp_activity.dv_load_date_time,
       p_exerp_activity.dv_load_end_date_time
  from dbo.h_exerp_activity
  join dbo.p_exerp_activity
    on h_exerp_activity.bk_hash = p_exerp_activity.bk_hash
  join #p_exerp_activity_insert
    on p_exerp_activity.bk_hash = #p_exerp_activity_insert.bk_hash
   and p_exerp_activity.p_exerp_activity_id = #p_exerp_activity_insert.p_exerp_activity_id
  join dbo.l_exerp_activity
    on p_exerp_activity.bk_hash = l_exerp_activity.bk_hash
   and p_exerp_activity.l_exerp_activity_id = l_exerp_activity.l_exerp_activity_id
  join dbo.l_exerp_activity_1
    on p_exerp_activity.bk_hash = l_exerp_activity_1.bk_hash
   and p_exerp_activity.l_exerp_activity_1_id = l_exerp_activity_1.l_exerp_activity_1_id
  join dbo.l_exerp_activity_2
    on p_exerp_activity.bk_hash = l_exerp_activity_2.bk_hash
   and p_exerp_activity.l_exerp_activity_2_id = l_exerp_activity_2.l_exerp_activity_2_id
  join dbo.s_exerp_activity
    on p_exerp_activity.bk_hash = s_exerp_activity.bk_hash
   and p_exerp_activity.s_exerp_activity_id = s_exerp_activity.s_exerp_activity_id
  join dbo.s_exerp_activity_1
    on p_exerp_activity.bk_hash = s_exerp_activity_1.bk_hash
   and p_exerp_activity.s_exerp_activity_1_id = s_exerp_activity_1.s_exerp_activity_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_activity
   where d_exerp_activity.bk_hash in (select bk_hash from #p_exerp_activity_insert)

  insert dbo.d_exerp_activity(
             bk_hash,
             activity_id,
             access_group_id,
             activity_name,
             activity_state,
             activity_type,
             age_group_id,
             color,
             course_schedule_type,
             d_exerp_activity_group_bk_hash,
             d_exerp_age_group_bk_hash,
             description,
             dim_boss_product_key,
             dim_exerp_time_configuration_key,
             external_id,
             max_participants,
             max_waiting_list_participants,
             time_configuration_id,
             deleted_flag,
             p_exerp_activity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         activity_id,
         access_group_id,
         activity_name,
         activity_state,
         activity_type,
         age_group_id,
         color,
         course_schedule_type,
         d_exerp_activity_group_bk_hash,
         d_exerp_age_group_bk_hash,
         description,
         dim_boss_product_key,
         dim_exerp_time_configuration_key,
         external_id,
         max_participants,
         max_waiting_list_participants,
         time_configuration_id,
         dv_deleted,
         p_exerp_activity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_activity)
--Done!
end
