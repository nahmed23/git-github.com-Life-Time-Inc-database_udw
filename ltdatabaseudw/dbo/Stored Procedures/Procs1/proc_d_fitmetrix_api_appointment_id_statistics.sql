CREATE PROC [dbo].[proc_d_fitmetrix_api_appointment_id_statistics] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_appointment_id_statistics)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_fitmetrix_api_appointment_id_statistics_insert') is not null drop table #p_fitmetrix_api_appointment_id_statistics_insert
create table dbo.#p_fitmetrix_api_appointment_id_statistics_insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_appointment_id_statistics.p_fitmetrix_api_appointment_id_statistics_id,
       p_fitmetrix_api_appointment_id_statistics.bk_hash
  from dbo.p_fitmetrix_api_appointment_id_statistics
 where p_fitmetrix_api_appointment_id_statistics.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_fitmetrix_api_appointment_id_statistics.dv_batch_id > @max_dv_batch_id
        or p_fitmetrix_api_appointment_id_statistics.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_appointment_id_statistics.bk_hash,
       p_fitmetrix_api_appointment_id_statistics.bk_hash fact_fitmetrix_appointment_detail_key,
       p_fitmetrix_api_appointment_id_statistics.profile_appointment_id profile_appointment_id,
       s_fitmetrix_api_appointment_id_statistics.appointment_name appointment_name,
       case when s_fitmetrix_api_appointment_id_statistics.checked_in = 'true' then 'Y'
     else 'N'
 end checked_in_flag,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.create_date is null then '-998'
     when s_fitmetrix_api_appointment_id_statistics.create_date = '0001-01-01T00:00:00' then '-998'
     else convert(varchar, convert(datetime,s_fitmetrix_api_appointment_id_statistics.create_date,126), 112)
 end created_dim_date_key,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.create_date is null then '-998'
     when s_fitmetrix_api_appointment_id_statistics.create_date = '0001-01-01T00:00:00' then '-998'
     else '1' + replace(substring(convert(varchar,convert(datetime,s_fitmetrix_api_appointment_id_statistics.create_date,126),114), 1, 5),':','')
 end created_dim_time_key,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when l_fitmetrix_api_appointment_id_statistics.appointment_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_fitmetrix_api_appointment_id_statistics.appointment_id as varchar(500)),'z#@$k%&P'))),2)
 end dim_fitmetrix_appointment_key,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when l_fitmetrix_api_appointment_id_statistics.external_id is null then '-998'
     when len(l_fitmetrix_api_appointment_id_statistics.external_id) != 9 then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_fitmetrix_api_appointment_id_statistics.external_id as varchar(500)),'z#@$k%&P'))),2)
 end dim_mms_member_key,
       s_fitmetrix_api_appointment_id_statistics.email email_address,
       s_fitmetrix_api_appointment_id_statistics.first_name first_name,
       s_fitmetrix_api_appointment_id_statistics.last_name last_name,
       s_fitmetrix_api_appointment_id_statistics.spot_number spot_number,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.start_date_time is null then '-998'
     else convert(varchar, convert(datetime,s_fitmetrix_api_appointment_id_statistics.start_date_time,126), 112)
 end start_dim_date_key,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.start_date_time is null then '-998'
     else '1' + replace(substring(convert(varchar,convert(datetime,s_fitmetrix_api_appointment_id_statistics.start_date_time,126),114), 1, 5),':','')
 end start_dim_time_key,
       s_fitmetrix_api_appointment_id_statistics.total_points total_points,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.waitlist_date_time is null then '-998'
     when s_fitmetrix_api_appointment_id_statistics.waitlist_date_time = '1900-01-01T00:00:00' then '-998'
     else convert(varchar, convert(datetime,s_fitmetrix_api_appointment_id_statistics.waitlist_date_time,126), 112)
 end waitlist_dim_date_key,
       case when p_fitmetrix_api_appointment_id_statistics.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointment_id_statistics.bk_hash
     when s_fitmetrix_api_appointment_id_statistics.waitlist_date_time is null then '-998'
     when s_fitmetrix_api_appointment_id_statistics.waitlist_date_time = '1900-01-01T00:00:00' then '-998'
     else '1' + replace(substring(convert(varchar,convert(datetime,s_fitmetrix_api_appointment_id_statistics.waitlist_date_time,126),114), 1, 5),':','')
 end waitlist_dim_time_key,
       case when s_fitmetrix_api_appointment_id_statistics.waitlist = 'true' then 'Y'
     else 'N'
 end waitlist_flag,
       s_fitmetrix_api_appointment_id_statistics.waitlist_position waitlist_position,
       h_fitmetrix_api_appointment_id_statistics.dv_deleted,
       p_fitmetrix_api_appointment_id_statistics.p_fitmetrix_api_appointment_id_statistics_id,
       p_fitmetrix_api_appointment_id_statistics.dv_batch_id,
       p_fitmetrix_api_appointment_id_statistics.dv_load_date_time,
       p_fitmetrix_api_appointment_id_statistics.dv_load_end_date_time
  from dbo.h_fitmetrix_api_appointment_id_statistics
  join dbo.p_fitmetrix_api_appointment_id_statistics
    on h_fitmetrix_api_appointment_id_statistics.bk_hash = p_fitmetrix_api_appointment_id_statistics.bk_hash  join #p_fitmetrix_api_appointment_id_statistics_insert
    on p_fitmetrix_api_appointment_id_statistics.bk_hash = #p_fitmetrix_api_appointment_id_statistics_insert.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.p_fitmetrix_api_appointment_id_statistics_id = #p_fitmetrix_api_appointment_id_statistics_insert.p_fitmetrix_api_appointment_id_statistics_id
  join dbo.l_fitmetrix_api_appointment_id_statistics
    on p_fitmetrix_api_appointment_id_statistics.bk_hash = l_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id = l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id
  join dbo.s_fitmetrix_api_appointment_id_statistics
    on p_fitmetrix_api_appointment_id_statistics.bk_hash = s_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id = s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_fitmetrix_api_appointment_id_statistics
   where d_fitmetrix_api_appointment_id_statistics.bk_hash in (select bk_hash from #p_fitmetrix_api_appointment_id_statistics_insert)

  insert dbo.d_fitmetrix_api_appointment_id_statistics(
             bk_hash,
             fact_fitmetrix_appointment_detail_key,
             profile_appointment_id,
             appointment_name,
             checked_in_flag,
             created_dim_date_key,
             created_dim_time_key,
             dim_fitmetrix_appointment_key,
             dim_mms_member_key,
             email_address,
             first_name,
             last_name,
             spot_number,
             start_dim_date_key,
             start_dim_time_key,
             total_points,
             waitlist_dim_date_key,
             waitlist_dim_time_key,
             waitlist_flag,
             waitlist_position,
             deleted_flag,
             p_fitmetrix_api_appointment_id_statistics_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_fitmetrix_appointment_detail_key,
         profile_appointment_id,
         appointment_name,
         checked_in_flag,
         created_dim_date_key,
         created_dim_time_key,
         dim_fitmetrix_appointment_key,
         dim_mms_member_key,
         email_address,
         first_name,
         last_name,
         spot_number,
         start_dim_date_key,
         start_dim_time_key,
         total_points,
         waitlist_dim_date_key,
         waitlist_dim_time_key,
         waitlist_flag,
         waitlist_position,
         dv_deleted,
         p_fitmetrix_api_appointment_id_statistics_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_appointment_id_statistics)
--Done!
end
