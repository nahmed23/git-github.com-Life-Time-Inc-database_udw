CREATE PROC [dbo].[proc_d_mms_membership] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_membership_insert') is not null drop table #p_mms_membership_insert
create table dbo.#p_mms_membership_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership.p_mms_membership_id,
       p_mms_membership.bk_hash
  from dbo.p_mms_membership
 where p_mms_membership.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_membership.dv_batch_id > @max_dv_batch_id
        or p_mms_membership.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership.bk_hash,
       p_mms_membership.bk_hash dim_mms_membership_key,
       p_mms_membership.membership_id membership_id,
       l_mms_membership.advisor_employee_id advisor_employee_id,
       l_mms_membership.club_id club_id,
       l_mms_membership.company_id company_id,
       s_mms_membership.created_date_time created_date_time,
       case when p_mms_membership.bk_hash in ('-997', '-998', '-999') then p_mms_membership.bk_hash
       when s_mms_membership.created_date_time is null then '-998'
        else convert(varchar, s_mms_membership.created_date_time, 112)  end created_date_time_key,
       l_mms_membership.crm_opportunity_id crm_opportunity_id,
       s_mms_membership.current_price current_price,
       case when p_mms_membership.bk_hash in ('-997', '-998', '-999')  then p_mms_membership.bk_hash      
       when l_mms_membership.crm_opportunity_id is null then '-998'        
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.crm_opportunity_id as varchar(500)),'z#@$k%&P'))),2) end dim_crm_opportunity_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.company_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.company_id as int) as varchar(500)),'z#@$k%&P'))),2)
       			end dim_mms_company_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.membership_type_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.membership_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	         end dim_mms_membership_type_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_eft_option_id is null then '-998'
            else 'r_mms_val_eft_option_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_eft_option_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end eft_option_dim_description_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_enrollment_type_id is null then '-998'
            else 'r_mms_val_enrollment_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_enrollment_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end enrollment_type_dim_description_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.club_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.club_id as int) as varchar(500)),'z#@$k%&P'))),2)          
                end home_dim_club_key,
       case when s_mms_membership.activation_date >= convert(datetime, '2100.01.01', 102) then convert(datetime, '9999.12.31', 102)
                     else s_mms_membership.activation_date
                end membership_activation_date,
       s_mms_membership.cancellation_request_date membership_cancellation_request_date,
       s_mms_membership.created_date_time membership_created_date_time,
       convert(int,convert(varchar,s_mms_membership.created_date_time,112)) membership_created_dim_date_key,
       case when s_mms_membership.expiration_date >= convert(datetime, '2100.01.01', 102) then convert(datetime, '9999.12.31', 102)
                     else s_mms_membership.expiration_date
                end membership_expiration_date,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_membership_source_id is null then '-998'
            else 'r_mms_val_membership_source_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_membership_source_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end membership_source_dim_description_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_membership_status_id is null then '-998'
            else 'r_mms_val_membership_status_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_membership_status_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end membership_status_dim_description_key,
       l_mms_membership.membership_type_id membership_type_id,
       case when l_mms_membership.val_termination_reason_id in (24,35) then 'Y'
                     else 'N'
                end non_payment_termination_flag,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.prior_plus_membership_type_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.advisor_employee_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end original_sales_dim_employee_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.prior_plus_membership_type_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.prior_plus_membership_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       		end prior_plus_dim_membership_type_key,
       l_mms_membership.prior_plus_membership_type_id prior_plus_membership_type_id,
       s_mms_membership.prior_plus_price prior_plus_price,
       s_mms_membership_1.prior_plus_undiscounted_price prior_plus_undiscounted_price,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_termination_reason_club_type_id is null then '-998'
            else 'r_mms_val_termination_reason_club_type'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_termination_reason_club_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end termination_reason_club_type_dim_description_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
            when l_mms_membership.val_termination_reason_id is null then '-998'
            else 'r_mms_val_termination_reason_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.val_termination_reason_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end termination_reason_dim_description_key,
       s_mms_membership_1.undiscounted_price undiscounted_price,
       l_mms_membership.val_eft_option_id val_eft_option_id,
       l_mms_membership.val_enrollment_type_id val_enrollment_type_id,
       l_mms_membership.val_membership_source_id val_membership_source_id,
       l_mms_membership.val_membership_status_id val_membership_status_id,
       l_mms_membership.val_termination_reason_club_type_id val_termination_reason_club_type_id,
       l_mms_membership.val_termination_reason_id val_termination_reason_id,
       isnull(h_mms_membership.dv_deleted,0) dv_deleted,
       p_mms_membership.p_mms_membership_id,
       p_mms_membership.dv_batch_id,
       p_mms_membership.dv_load_date_time,
       p_mms_membership.dv_load_end_date_time
  from dbo.h_mms_membership
  join dbo.p_mms_membership
    on h_mms_membership.bk_hash = p_mms_membership.bk_hash
  join #p_mms_membership_insert
    on p_mms_membership.bk_hash = #p_mms_membership_insert.bk_hash
   and p_mms_membership.p_mms_membership_id = #p_mms_membership_insert.p_mms_membership_id
  join dbo.l_mms_membership
    on p_mms_membership.bk_hash = l_mms_membership.bk_hash
   and p_mms_membership.l_mms_membership_id = l_mms_membership.l_mms_membership_id
  join dbo.l_mms_membership_1
    on p_mms_membership.bk_hash = l_mms_membership_1.bk_hash
   and p_mms_membership.l_mms_membership_1_id = l_mms_membership_1.l_mms_membership_1_id
  join dbo.s_mms_membership
    on p_mms_membership.bk_hash = s_mms_membership.bk_hash
   and p_mms_membership.s_mms_membership_id = s_mms_membership.s_mms_membership_id
  join dbo.s_mms_membership_1
    on p_mms_membership.bk_hash = s_mms_membership_1.bk_hash
   and p_mms_membership.s_mms_membership_1_id = s_mms_membership_1.s_mms_membership_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_membership
   where d_mms_membership.bk_hash in (select bk_hash from #p_mms_membership_insert)

  insert dbo.d_mms_membership(
             bk_hash,
             dim_mms_membership_key,
             membership_id,
             advisor_employee_id,
             club_id,
             company_id,
             created_date_time,
             created_date_time_key,
             crm_opportunity_id,
             current_price,
             dim_crm_opportunity_key,
             dim_mms_company_key,
             dim_mms_membership_type_key,
             eft_option_dim_description_key,
             enrollment_type_dim_description_key,
             home_dim_club_key,
             membership_activation_date,
             membership_cancellation_request_date,
             membership_created_date_time,
             membership_created_dim_date_key,
             membership_expiration_date,
             membership_source_dim_description_key,
             membership_status_dim_description_key,
             membership_type_id,
             non_payment_termination_flag,
             original_sales_dim_employee_key,
             prior_plus_dim_membership_type_key,
             prior_plus_membership_type_id,
             prior_plus_price,
             prior_plus_undiscounted_price,
             termination_reason_club_type_dim_description_key,
             termination_reason_dim_description_key,
             undiscounted_price,
             val_eft_option_id,
             val_enrollment_type_id,
             val_membership_source_id,
             val_membership_status_id,
             val_termination_reason_club_type_id,
             val_termination_reason_id,
             deleted_flag,
             p_mms_membership_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_membership_key,
         membership_id,
         advisor_employee_id,
         club_id,
         company_id,
         created_date_time,
         created_date_time_key,
         crm_opportunity_id,
         current_price,
         dim_crm_opportunity_key,
         dim_mms_company_key,
         dim_mms_membership_type_key,
         eft_option_dim_description_key,
         enrollment_type_dim_description_key,
         home_dim_club_key,
         membership_activation_date,
         membership_cancellation_request_date,
         membership_created_date_time,
         membership_created_dim_date_key,
         membership_expiration_date,
         membership_source_dim_description_key,
         membership_status_dim_description_key,
         membership_type_id,
         non_payment_termination_flag,
         original_sales_dim_employee_key,
         prior_plus_dim_membership_type_key,
         prior_plus_membership_type_id,
         prior_plus_price,
         prior_plus_undiscounted_price,
         termination_reason_club_type_dim_description_key,
         termination_reason_dim_description_key,
         undiscounted_price,
         val_eft_option_id,
         val_enrollment_type_id,
         val_membership_source_id,
         val_membership_status_id,
         val_termination_reason_club_type_id,
         val_termination_reason_id,
         dv_deleted,
         p_mms_membership_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership)
--Done!
end
