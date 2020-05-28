CREATE PROC [dbo].[proc_d_ec_notifications] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_notifications)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_notifications_insert') is not null drop table #p_ec_notifications_insert
create table dbo.#p_ec_notifications_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_notifications.p_ec_notifications_id,
       p_ec_notifications.bk_hash
  from dbo.p_ec_notifications
 where p_ec_notifications.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_notifications.dv_batch_id > @max_dv_batch_id
        or p_ec_notifications.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_notifications.bk_hash,
       p_ec_notifications.bk_hash fact_trainerize_notification_key,
       p_ec_notifications.notification_id notification_id,
       case when p_ec_notifications.bk_hash in ('-997', '-998', '-999') then p_ec_notifications.bk_hash   
       when s_ec_notifications.created_date is null then '-998'   
       else convert(char(8), s_ec_notifications.created_date, 112)   end created_dim_date_key,
       case when p_ec_notifications.bk_hash in ('-997','-998','-999') then p_ec_notifications.bk_hash
       when s_ec_notifications.created_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_notifications.created_date,114), 1, 5),':','') end created_dim_time_key,
       s_ec_notifications.message message,
       case when s_ec_notifications.message_type = 1  then 'Y' else 'N' end message_type_flag,
       s_ec_notifications.[from] notifications_from,
       s_ec_notifications.[to] notifications_to,
       case when p_ec_notifications.bk_hash in ('-997', '-998', '-999') then p_ec_notifications.bk_hash   
       when s_ec_notifications.received is null then '-998'   
       else convert(char(8), s_ec_notifications.received, 112)   end received_dim_date_key,
       case when p_ec_notifications.bk_hash in ('-997','-998','-999') then p_ec_notifications.bk_hash
       when s_ec_notifications.received is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_notifications.received,114), 1, 5),':','') end received_dim_time_key,
       l_ec_notifications.source_id source_id,
       l_ec_notifications.source_thread_id source_thread_id,
       s_ec_notifications.source_type source_type,
       case when s_ec_notifications.status = 1  then 'Y' else 'N' end status_flag,
       s_ec_notifications.subject subject,
       case when p_ec_notifications.bk_hash in ('-997', '-998', '-999') then p_ec_notifications.bk_hash   
       when s_ec_notifications.updated_date is null then '-998'   
       else convert(char(8), s_ec_notifications.updated_date, 112)   end updated_dim_date_key,
       case when p_ec_notifications.bk_hash in ('-997','-998','-999') then p_ec_notifications.bk_hash
       when s_ec_notifications.updated_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_notifications.updated_date,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_ec_notifications.dv_deleted,0) dv_deleted,
       p_ec_notifications.p_ec_notifications_id,
       p_ec_notifications.dv_batch_id,
       p_ec_notifications.dv_load_date_time,
       p_ec_notifications.dv_load_end_date_time
  from dbo.h_ec_notifications
  join dbo.p_ec_notifications
    on h_ec_notifications.bk_hash = p_ec_notifications.bk_hash
  join #p_ec_notifications_insert
    on p_ec_notifications.bk_hash = #p_ec_notifications_insert.bk_hash
   and p_ec_notifications.p_ec_notifications_id = #p_ec_notifications_insert.p_ec_notifications_id
  join dbo.l_ec_notifications
    on p_ec_notifications.bk_hash = l_ec_notifications.bk_hash
   and p_ec_notifications.l_ec_notifications_id = l_ec_notifications.l_ec_notifications_id
  join dbo.s_ec_notifications
    on p_ec_notifications.bk_hash = s_ec_notifications.bk_hash
   and p_ec_notifications.s_ec_notifications_id = s_ec_notifications.s_ec_notifications_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_notifications
   where d_ec_notifications.bk_hash in (select bk_hash from #p_ec_notifications_insert)

  insert dbo.d_ec_notifications(
             bk_hash,
             fact_trainerize_notification_key,
             notification_id,
             created_dim_date_key,
             created_dim_time_key,
             message,
             message_type_flag,
             notifications_from,
             notifications_to,
             received_dim_date_key,
             received_dim_time_key,
             source_id,
             source_thread_id,
             source_type,
             status_flag,
             subject,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_ec_notifications_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_trainerize_notification_key,
         notification_id,
         created_dim_date_key,
         created_dim_time_key,
         message,
         message_type_flag,
         notifications_from,
         notifications_to,
         received_dim_date_key,
         received_dim_time_key,
         source_id,
         source_thread_id,
         source_type,
         status_flag,
         subject,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_ec_notifications_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_notifications)
--Done!
end
