CREATE PROC [dbo].[proc_d_mms_email_address_status] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_email_address_status)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_email_address_status_insert') is not null drop table #p_mms_email_address_status_insert
create table dbo.#p_mms_email_address_status_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_email_address_status.p_mms_email_address_status_id,
       p_mms_email_address_status.bk_hash
  from dbo.p_mms_email_address_status
 where p_mms_email_address_status.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_email_address_status.dv_batch_id > @max_dv_batch_id
        or p_mms_email_address_status.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_email_address_status.bk_hash,
       p_mms_email_address_status.bk_hash dim_mms_email_address_status_key,
       p_mms_email_address_status.email_address_status_id email_address_status_id,
       isnull(s_mms_email_address_status.email_address,'') email_address,
       isnull(s_mms_email_address_status.email_address_search,'') email_address_search,
       s_mms_email_address_status.status_from_date status_from_date,
       case when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
       when s_mms_email_address_status.status_from_date is null then '-998'    
       else convert(varchar, s_mms_email_address_status.status_from_date, 112)   end status_from_dim_date_key,
       case  when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
       when s_mms_email_address_status.status_from_date is null then '-998'     
       else '1' + replace(substring(convert(varchar,s_mms_email_address_status.status_from_date,114), 1, 5),':','')   end status_from_dim_time_key,
       s_mms_email_address_status.status_thru_date status_thru_date,
       case when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
       when s_mms_email_address_status.status_thru_date is null then '-998'    
       else convert(varchar, s_mms_email_address_status.status_thru_date, 112)   end status_thru_dim_date_key,
       case  when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
       when s_mms_email_address_status.status_thru_date is null then '-998'     
       else '1' + replace(substring(convert(varchar,s_mms_email_address_status.status_thru_date,114), 1, 5),':','')   end status_thru_dim_time_key,
       case when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
      when l_mms_email_address_status.val_communication_preference_source_id is null then '-998'  else concat('r_mms_val_communication_preference_source_',convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_email_address_status.val_communication_preference_source_id as varchar(500)),'z#@$k%&P'))),2)) end val_communication_preference_source_key,
       case when p_mms_email_address_status.bk_hash in ('-997', '-998', '-999') then p_mms_email_address_status.bk_hash 
      when l_mms_email_address_status.val_communication_preference_status_id is null then '-998' else concat('r_mms_val_communication_preference_status_', convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_email_address_status.val_communication_preference_status_id as varchar(500)),'z#@$k%&P'))),2)) end val_communication_preference_status_key,
       h_mms_email_address_status.dv_deleted,
       p_mms_email_address_status.p_mms_email_address_status_id,
       p_mms_email_address_status.dv_batch_id,
       p_mms_email_address_status.dv_load_date_time,
       p_mms_email_address_status.dv_load_end_date_time
  from dbo.h_mms_email_address_status
  join dbo.p_mms_email_address_status
    on h_mms_email_address_status.bk_hash = p_mms_email_address_status.bk_hash  join #p_mms_email_address_status_insert
    on p_mms_email_address_status.bk_hash = #p_mms_email_address_status_insert.bk_hash
   and p_mms_email_address_status.p_mms_email_address_status_id = #p_mms_email_address_status_insert.p_mms_email_address_status_id
  join dbo.l_mms_email_address_status
    on p_mms_email_address_status.bk_hash = l_mms_email_address_status.bk_hash
   and p_mms_email_address_status.l_mms_email_address_status_id = l_mms_email_address_status.l_mms_email_address_status_id
  join dbo.s_mms_email_address_status
    on p_mms_email_address_status.bk_hash = s_mms_email_address_status.bk_hash
   and p_mms_email_address_status.s_mms_email_address_status_id = s_mms_email_address_status.s_mms_email_address_status_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_email_address_status
   where d_mms_email_address_status.bk_hash in (select bk_hash from #p_mms_email_address_status_insert)

  insert dbo.d_mms_email_address_status(
             bk_hash,
             dim_mms_email_address_status_key,
             email_address_status_id,
             email_address,
             email_address_search,
             status_from_date,
             status_from_dim_date_key,
             status_from_dim_time_key,
             status_thru_date,
             status_thru_dim_date_key,
             status_thru_dim_time_key,
             val_communication_preference_source_key,
             val_communication_preference_status_key,
             deleted_flag,
             p_mms_email_address_status_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_email_address_status_key,
         email_address_status_id,
         email_address,
         email_address_search,
         status_from_date,
         status_from_dim_date_key,
         status_from_dim_time_key,
         status_thru_date,
         status_thru_dim_date_key,
         status_thru_dim_time_key,
         val_communication_preference_source_key,
         val_communication_preference_status_key,
         dv_deleted,
         p_mms_email_address_status_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_email_address_status)
--Done!
end
