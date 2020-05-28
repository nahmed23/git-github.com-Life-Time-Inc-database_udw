CREATE PROC [dbo].[proc_d_mms_membership_modification_request] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_modification_request)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_membership_modification_request_insert') is not null drop table #p_mms_membership_modification_request_insert
create table dbo.#p_mms_membership_modification_request_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_modification_request.p_mms_membership_modification_request_id,
       p_mms_membership_modification_request.bk_hash
  from dbo.p_mms_membership_modification_request
 where p_mms_membership_modification_request.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_membership_modification_request.dv_batch_id > @max_dv_batch_id
        or p_mms_membership_modification_request.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_modification_request.bk_hash,
       p_mms_membership_modification_request.membership_modification_request_id membership_modification_request_id,
       s_mms_membership_modification_request.add_on_fee add_on_fee,
       s_mms_membership_modification_request.agreement_price agreement_price,
       l_mms_membership_modification_request.club_id club_id,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.commisioned_employee_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.commisioned_employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end commisioned_dim_employee_key,
       l_mms_membership_modification_request.commisioned_employee_id commisioned_employee_id,
       s_mms_membership_modification_request.deactivated_members deactivated_members,
       s_mms_membership_modification_request.diamond_fee diamond_fee,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.club_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.club_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_club_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.employee_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_employee_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.member_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.membership_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_membership_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.membership_type_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.membership_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_membership_type_key,
       s_mms_membership_modification_request.effective_date effective_date,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.effective_date is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.effective_date, 112)    end effective_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.effective_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.effective_date,114), 1, 5),':','') end effective_dim_time_key,
       l_mms_membership_modification_request.employee_id employee_id,
       s_mms_membership_modification_request.first_months_dues first_months_dues,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.val_flex_reason_id is null then '-998' else concat('r_mms_val_flex_reason_',
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.val_flex_reason_id as int) as varchar(500)),'z#@$k%&P'))),2))   end flex_reason_dim_description_key,
       case when s_mms_membership_modification_request.full_access_date_extension_flag = 1 then 'Y' else 'N' end full_access_date_extension_flag,
       case when s_mms_membership_modification_request.future_membership_upgrade_flag = 1 then 'Y' else 'N' end future_membership_upgrade_flag,
       s_mms_membership_modification_request.inserted_date_time inserted_date_time,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_mms_membership_modification_request.juniors_assessed juniors_assessed,
       s_mms_membership_modification_request.last_eft_month last_eft_month,
       l_mms_membership_modification_request.member_agreement_staging_id member_agreement_staging_id,
       case when s_mms_membership_modification_request.member_freeze_flag = 1 then 'Y' else 'N' end member_freeze_flag,
       l_mms_membership_modification_request.member_id member_id,
       l_mms_membership_modification_request.membership_id membership_id,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request_1.val_membership_modification_request_source_id is null then '-998' else concat('r_mms_val_membership_modification_request_source_',
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request_1.val_membership_modification_request_source_id as int) as varchar(500)),'z#@$k%&P'))),2)) end membership_modification_request_source_dim_description_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.val_membership_modification_request_status_id is null then '-998' else concat('r_mms_val_membership_modification_request_status_',
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.val_membership_modification_request_status_id as int) as varchar(500)),'z#@$k%&P'))),2))   end membership_modification_request_status_dim_description_key,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.val_membership_modification_request_type_id is null then '-998' else concat('r_mms_val_membership_modification_request_type_',
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.val_membership_modification_request_type_id as int) as varchar(500)),'z#@$k%&P'))),2))   end membership_modification_request_type_dim_description_key,
       l_mms_membership_modification_request.membership_type_id membership_type_id,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.val_membership_upgrade_date_range_id is null then '-998' else concat('r_mms_val_membership_upgrade_date_range_',
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.val_membership_upgrade_date_range_id as int) as varchar(500)),'z#@$k%&P'))),2)) end membership_upgrade_date_range_dim_description_key,
       s_mms_membership_modification_request.membership_upgrade_month_year membership_upgrade_month_year,
       s_mms_membership_modification_request.new_members new_members,
       l_mms_membership_modification_request_1.new_primary_id new_primary_id,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when l_mms_membership_modification_request.previous_membership_type_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_modification_request.previous_membership_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end previous_dim_mms_membership_type_key,
       l_mms_membership_modification_request.previous_membership_type_id previous_membership_type_id,
       s_mms_membership_modification_request.pro_rated_dues pro_rated_dues,
       s_mms_membership_modification_request.request_date_time request_date_time,
       s_mms_membership_modification_request.request_date_time_zone request_date_time_zone,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.request_date_time is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.request_date_time, 112)    end request_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.request_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.request_date_time,114), 1, 5),':','') end request_dim_time_key,
       s_mms_membership_modification_request.service_fee service_fee,
       s_mms_membership_modification_request.status_changed_date_time status_changed_date_time,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.status_changed_date_time is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.status_changed_date_time, 112)    end status_changed_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.status_changed_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.status_changed_date_time,114), 1, 5),':','') end status_changed_dim_time_key,
       s_mms_membership_modification_request.total_monthly_amount total_monthly_amount,
       s_mms_membership_modification_request_1.undiscounted_price undiscounted_price,
       s_mms_membership_modification_request.updated_date_time updated_date_time,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.updated_date_time is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_mms_membership_modification_request.utc_request_date_time utc_request_date_time,
       case when p_mms_membership_modification_request.bk_hash in('-997', '-998', '-999') then p_mms_membership_modification_request.bk_hash
           when s_mms_membership_modification_request.utc_request_date_time is null then '-998'
        else convert(varchar, s_mms_membership_modification_request.utc_request_date_time, 112)    end utc_request_dim_date_key,
       case when p_mms_membership_modification_request.bk_hash in ('-997','-998','-999') then p_mms_membership_modification_request.bk_hash
       when s_mms_membership_modification_request.utc_request_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_modification_request.utc_request_date_time,114), 1, 5),':','') end utc_request_dim_time_key,
       l_mms_membership_modification_request.val_flex_reason_id val_flex_reason_id,
       l_mms_membership_modification_request_1.val_membership_modification_request_source_id val_membership_modification_request_source_id,
       l_mms_membership_modification_request.val_membership_modification_request_status_id val_membership_modification_request_status_id,
       l_mms_membership_modification_request.val_membership_modification_request_type_id val_membership_modification_request_type_id,
       l_mms_membership_modification_request.val_membership_upgrade_date_range_id val_membership_upgrade_date_range_id,
       case when s_mms_membership_modification_request.waive_service_fee_flag = 1 then 'Y' else 'N' end waive_service_fee_flag,
       isnull(h_mms_membership_modification_request.dv_deleted,0) dv_deleted,
       p_mms_membership_modification_request.p_mms_membership_modification_request_id,
       p_mms_membership_modification_request.dv_batch_id,
       p_mms_membership_modification_request.dv_load_date_time,
       p_mms_membership_modification_request.dv_load_end_date_time
  from dbo.h_mms_membership_modification_request
  join dbo.p_mms_membership_modification_request
    on h_mms_membership_modification_request.bk_hash = p_mms_membership_modification_request.bk_hash
  join #p_mms_membership_modification_request_insert
    on p_mms_membership_modification_request.bk_hash = #p_mms_membership_modification_request_insert.bk_hash
   and p_mms_membership_modification_request.p_mms_membership_modification_request_id = #p_mms_membership_modification_request_insert.p_mms_membership_modification_request_id
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
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_membership_modification_request
   where d_mms_membership_modification_request.bk_hash in (select bk_hash from #p_mms_membership_modification_request_insert)

  insert dbo.d_mms_membership_modification_request(
             bk_hash,
             membership_modification_request_id,
             add_on_fee,
             agreement_price,
             club_id,
             commisioned_dim_employee_key,
             commisioned_employee_id,
             deactivated_members,
             diamond_fee,
             dim_club_key,
             dim_employee_key,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_mms_membership_type_key,
             effective_date,
             effective_dim_date_key,
             effective_dim_time_key,
             employee_id,
             first_months_dues,
             flex_reason_dim_description_key,
             full_access_date_extension_flag,
             future_membership_upgrade_flag,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             juniors_assessed,
             last_eft_month,
             member_agreement_staging_id,
             member_freeze_flag,
             member_id,
             membership_id,
             membership_modification_request_source_dim_description_key,
             membership_modification_request_status_dim_description_key,
             membership_modification_request_type_dim_description_key,
             membership_type_id,
             membership_upgrade_date_range_dim_description_key,
             membership_upgrade_month_year,
             new_members,
             new_primary_id,
             previous_dim_mms_membership_type_key,
             previous_membership_type_id,
             pro_rated_dues,
             request_date_time,
             request_date_time_zone,
             request_dim_date_key,
             request_dim_time_key,
             service_fee,
             status_changed_date_time,
             status_changed_dim_date_key,
             status_changed_dim_time_key,
             total_monthly_amount,
             undiscounted_price,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_request_date_time,
             utc_request_dim_date_key,
             utc_request_dim_time_key,
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
         commisioned_dim_employee_key,
         commisioned_employee_id,
         deactivated_members,
         diamond_fee,
         dim_club_key,
         dim_employee_key,
         dim_mms_member_key,
         dim_mms_membership_key,
         dim_mms_membership_type_key,
         effective_date,
         effective_dim_date_key,
         effective_dim_time_key,
         employee_id,
         first_months_dues,
         flex_reason_dim_description_key,
         full_access_date_extension_flag,
         future_membership_upgrade_flag,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         juniors_assessed,
         last_eft_month,
         member_agreement_staging_id,
         member_freeze_flag,
         member_id,
         membership_id,
         membership_modification_request_source_dim_description_key,
         membership_modification_request_status_dim_description_key,
         membership_modification_request_type_dim_description_key,
         membership_type_id,
         membership_upgrade_date_range_dim_description_key,
         membership_upgrade_month_year,
         new_members,
         new_primary_id,
         previous_dim_mms_membership_type_key,
         previous_membership_type_id,
         pro_rated_dues,
         request_date_time,
         request_date_time_zone,
         request_dim_date_key,
         request_dim_time_key,
         service_fee,
         status_changed_date_time,
         status_changed_dim_date_key,
         status_changed_dim_time_key,
         total_monthly_amount,
         undiscounted_price,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_request_date_time,
         utc_request_dim_date_key,
         utc_request_dim_time_key,
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
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_modification_request)
--Done!
end
