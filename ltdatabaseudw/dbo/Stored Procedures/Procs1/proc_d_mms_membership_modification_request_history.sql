CREATE PROC [dbo].[proc_d_mms_membership_modification_request_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_modification_request_history);

if object_id('tempdb..#p_mms_membership_modification_request_id_list') is not null drop table #p_mms_membership_modification_request_id_list
create table dbo.#p_mms_membership_modification_request_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_membership_modification_request_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_membership_modification_request_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_membership_modification_request_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_membership_modification_request_id,bk_hash) as
(
    select d_mms_membership_modification_request_history.p_mms_membership_modification_request_id,
           d_mms_membership_modification_request_history.bk_hash
      from dbo.d_mms_membership_modification_request_history
      join undo_delete
        on d_mms_membership_modification_request_history.bk_hash = undo_delete.bk_hash
       and d_mms_membership_modification_request_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_membership_modification_request_insert (p_mms_membership_modification_request_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_membership_modification_request_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_membership_modification_request
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_membership_modification_request_update (p_mms_membership_modification_request_id,bk_hash) as
(
    select p_mms_membership_modification_request.p_mms_membership_modification_request_id,
           p_mms_membership_modification_request.bk_hash
      from dbo.p_mms_membership_modification_request
      join p_mms_membership_modification_request_insert
        on p_mms_membership_modification_request.bk_hash = p_mms_membership_modification_request_insert.bk_hash
       and p_mms_membership_modification_request.dv_load_end_date_time = p_mms_membership_modification_request_insert.dv_load_date_time
)
select undo_delete.p_mms_membership_modification_request_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_membership_modification_request_id,
       bk_hash
  from undo_update
union
select p_mms_membership_modification_request_insert.p_mms_membership_modification_request_id,
       bk_hash
  from p_mms_membership_modification_request_insert
union
select p_mms_membership_modification_request_update.p_mms_membership_modification_request_id,
       bk_hash
  from p_mms_membership_modification_request_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_membership_modification_request_id_list.bk_hash,
       p_mms_membership_modification_request.membership_modification_request_id membership_modification_request_id,
       s_mms_membership_modification_request.add_on_fee add_on_fee,
       s_mms_membership_modification_request.agreement_price agreement_price,
       l_mms_membership_modification_request.club_id club_id,
       l_mms_membership_modification_request.commisioned_employee_id commisioned_employee_id,
       s_mms_membership_modification_request.deactivated_members deactivated_members,
       s_mms_membership_modification_request.diamond_fee diamond_fee,
       s_mms_membership_modification_request.effective_date effective_date,
       l_mms_membership_modification_request.employee_id employee_id,
       s_mms_membership_modification_request.first_months_dues first_months_dues,
       s_mms_membership_modification_request.full_access_date_extension_flag full_access_date_extension_flag,
       s_mms_membership_modification_request.future_membership_upgrade_flag future_membership_upgrade_flag,
       s_mms_membership_modification_request.inserted_date_time inserted_date_time,
       s_mms_membership_modification_request.juniors_assessed juniors_assessed,
       s_mms_membership_modification_request.last_eft_month last_eft_month,
       l_mms_membership_modification_request.member_agreement_staging_id member_agreement_staging_id,
       s_mms_membership_modification_request.member_freeze_flag member_freeze_flag,
       l_mms_membership_modification_request.member_id member_id,
       l_mms_membership_modification_request.membership_id membership_id,
       l_mms_membership_modification_request.membership_type_id membership_type_id,
       s_mms_membership_modification_request.membership_upgrade_month_year membership_upgrade_month_year,
       s_mms_membership_modification_request.new_members new_members,
       l_mms_membership_modification_request_1.new_primary_id new_primary_id,
       l_mms_membership_modification_request.previous_membership_type_id previous_membership_type_id,
       s_mms_membership_modification_request.pro_rated_dues pro_rated_dues,
       s_mms_membership_modification_request.request_date_time request_date_time,
       s_mms_membership_modification_request.request_date_time_zone request_date_time_zone,
       s_mms_membership_modification_request.service_fee service_fee,
       s_mms_membership_modification_request.status_changed_date_time status_changed_date_time,
       s_mms_membership_modification_request.total_monthly_amount total_monthly_amount,
       s_mms_membership_modification_request_1.undiscounted_price undiscounted_price,
       s_mms_membership_modification_request.updated_date_time updated_date_time,
       s_mms_membership_modification_request.utc_request_date_time utc_request_date_time,
       l_mms_membership_modification_request.val_flex_reason_id val_flex_reason_id,
       l_mms_membership_modification_request_1.val_membership_modification_request_source_id val_membership_modification_request_source_id,
       l_mms_membership_modification_request.val_membership_modification_request_status_id val_membership_modification_request_status_id,
       l_mms_membership_modification_request.val_membership_modification_request_type_id val_membership_modification_request_type_id,
       l_mms_membership_modification_request.val_membership_upgrade_date_range_id val_membership_upgrade_date_range_id,
       s_mms_membership_modification_request.waive_service_fee_flag waive_service_fee_flag,
       h_mms_membership_modification_request.dv_deleted,
       p_mms_membership_modification_request.p_mms_membership_modification_request_id,
       p_mms_membership_modification_request.dv_batch_id,
       p_mms_membership_modification_request.dv_load_date_time,
       p_mms_membership_modification_request.dv_load_end_date_time
  from dbo.h_mms_membership_modification_request
  join dbo.p_mms_membership_modification_request
    on h_mms_membership_modification_request.bk_hash = p_mms_membership_modification_request.bk_hash  join #p_mms_membership_modification_request_id_list
    on p_mms_membership_modification_request.p_mms_membership_modification_request_id = #p_mms_membership_modification_request_id_list.p_mms_membership_modification_request_id
   and p_mms_membership_modification_request.bk_hash = #p_mms_membership_modification_request_id_list.bk_hash
  join dbo.l_mms_membership_modification_request
    on p_mms_membership_modification_request.bk_hash = l_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.l_mms_membership_modification_request_id = l_mms_membership_modification_request.l_mms_membership_modification_request_id
  join dbo.l_mms_membership_modification_request_1
    on p_mms_membership_modification_request.bk_hash = l_mms_membership_modification_request_1.bk_hash
   and p_mms_membership_modification_request.l_mms_membership_modification_request_1_id = l_mms_membership_modification_request_1.l_mms_membership_modification_request_1_id
  join dbo.s_mms_membership_modification_request
    on p_mms_membership_modification_request.bk_hash = s_mms_membership_modification_request.bk_hash
   and p_mms_membership_modification_request.s_mms_membership_modification_request_id = s_mms_membership_modification_request.s_mms_membership_modification_request_id
  join dbo.s_mms_membership_modification_request_1
    on p_mms_membership_modification_request.bk_hash = s_mms_membership_modification_request_1.bk_hash
   and p_mms_membership_modification_request.s_mms_membership_modification_request_1_id = s_mms_membership_modification_request_1.s_mms_membership_modification_request_1_id

-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_membership_modification_request_history
       where d_mms_membership_modification_request_history.p_mms_membership_modification_request_id in (select p_mms_membership_modification_request_id from #p_mms_membership_modification_request_id_list)

      insert dbo.d_mms_membership_modification_request_history(
                 bk_hash,
                 membership_modification_request_id,
                 add_on_fee,
                 agreement_price,
                 club_id,
                 commisioned_employee_id,
                 deactivated_members,
                 diamond_fee,
                 effective_date,
                 employee_id,
                 first_months_dues,
                 full_access_date_extension_flag,
                 future_membership_upgrade_flag,
                 inserted_date_time,
                 juniors_assessed,
                 last_eft_month,
                 member_agreement_staging_id,
                 member_freeze_flag,
                 member_id,
                 membership_id,
                 membership_type_id,
                 membership_upgrade_month_year,
                 new_members,
                 new_primary_id,
                 previous_membership_type_id,
                 pro_rated_dues,
                 request_date_time,
                 request_date_time_zone,
                 service_fee,
                 status_changed_date_time,
                 total_monthly_amount,
                 undiscounted_price,
                 updated_date_time,
                 utc_request_date_time,
                 val_flex_reason_id,
                 val_membership_modification_request_source_id,
                 val_membership_modification_request_status_id,
                 val_membership_modification_request_type_id,
                 val_membership_upgrade_date_range_id,
                 waive_service_fee_flag,
                 deleted_flag,
                 p_mms_membership_modification_request_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             membership_modification_request_id,
             add_on_fee,
             agreement_price,
             club_id,
             commisioned_employee_id,
             deactivated_members,
             diamond_fee,
             effective_date,
             employee_id,
             first_months_dues,
             full_access_date_extension_flag,
             future_membership_upgrade_flag,
             inserted_date_time,
             juniors_assessed,
             last_eft_month,
             member_agreement_staging_id,
             member_freeze_flag,
             member_id,
             membership_id,
             membership_type_id,
             membership_upgrade_month_year,
             new_members,
             new_primary_id,
             previous_membership_type_id,
             pro_rated_dues,
             request_date_time,
             request_date_time_zone,
             service_fee,
             status_changed_date_time,
             total_monthly_amount,
             undiscounted_price,
             updated_date_time,
             utc_request_date_time,
             val_flex_reason_id,
             val_membership_modification_request_source_id,
             val_membership_modification_request_status_id,
             val_membership_modification_request_type_id,
             val_membership_upgrade_date_range_id,
             waive_service_fee_flag,
             dv_deleted,
             p_mms_membership_modification_request_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_modification_request_history)
--Done!
end
