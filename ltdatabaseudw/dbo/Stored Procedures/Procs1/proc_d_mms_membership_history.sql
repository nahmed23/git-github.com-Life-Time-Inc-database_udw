CREATE PROC [dbo].[proc_d_mms_membership_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_history);

if object_id('tempdb..#p_mms_membership_id_list') is not null drop table #p_mms_membership_id_list
create table dbo.#p_mms_membership_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_membership_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_membership_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_membership_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_membership_id,bk_hash) as
(
    select d_mms_membership_history.p_mms_membership_id,
           d_mms_membership_history.bk_hash
      from dbo.d_mms_membership_history
      join undo_delete
        on d_mms_membership_history.bk_hash = undo_delete.bk_hash
       and d_mms_membership_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_membership_insert (p_mms_membership_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_membership_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_membership
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_membership_update (p_mms_membership_id,bk_hash) as
(
    select p_mms_membership.p_mms_membership_id,
           p_mms_membership.bk_hash
      from dbo.p_mms_membership
      join p_mms_membership_insert
        on p_mms_membership.bk_hash = p_mms_membership_insert.bk_hash
       and p_mms_membership.dv_load_end_date_time = p_mms_membership_insert.dv_load_date_time
)
select undo_delete.p_mms_membership_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_membership_id,
       bk_hash
  from undo_update
union
select p_mms_membership_insert.p_mms_membership_id,
       bk_hash
  from p_mms_membership_insert
union
select p_mms_membership_update.p_mms_membership_id,
       bk_hash
  from p_mms_membership_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_membership_id_list.bk_hash,
       p_mms_membership.bk_hash dim_mms_membership_key,
       p_mms_membership.membership_id membership_id,
       isnull(p_mms_membership.dv_greatest_satellite_date_time, convert(datetime, s_mms_membership.created_date_time, 102)) effective_date_time,
       case when p_mms_membership.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_membership.dv_load_end_date_time
                     else p_mms_membership.dv_next_greatest_satellite_date_time
                end expiration_date_time,
       l_mms_membership.advisor_employee_id advisor_employee_id,
       l_mms_membership.club_id club_id,
       l_mms_membership.company_id company_id,
       s_mms_membership.created_date_time created_date_time,
       l_mms_membership.crm_opportunity_id crm_opportunity_id,
       s_mms_membership.current_price current_price,
       case when p_mms_membership.bk_hash in ('-997', '-998', '-999')  then p_mms_membership.bk_hash
           when l_mms_membership.crm_opportunity_id is null then '-998' 
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership.crm_opportunity_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_opportunity_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.company_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.company_id as int) as varchar(500)),'z#@$k%&P'))),2)
       			end dim_mms_company_key,
       case when p_mms_membership.bk_hash in ('-997','-998','-999') then p_mms_membership.bk_hash
                     when l_mms_membership.membership_type_id is null then '-998'
                     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.membership_type_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	         end dim_mms_membership_type_key,
       p_mms_membership.dv_first_in_key_series dv_first_in_key_series,
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
       case when p_mms_membership.bk_hash in ('-997', '-998', '-999')  then p_mms_membership.bk_hash
           when l_mms_membership.advisor_employee_id is null then '-998' 
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership.advisor_employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end original_sales_dim_employee_key,
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
       s_mms_membership.updated_date_time updated_date_time,
       l_mms_membership.val_eft_option_id val_eft_option_id,
       l_mms_membership.val_enrollment_type_id val_enrollment_type_id,
       l_mms_membership.val_membership_source_id val_membership_source_id,
       l_mms_membership.val_membership_status_id val_membership_status_id,
       l_mms_membership.val_termination_reason_club_type_id val_termination_reason_club_type_id,
       l_mms_membership.val_termination_reason_id val_termination_reason_id,
       h_mms_membership.dv_deleted,
       p_mms_membership.p_mms_membership_id,
       p_mms_membership.dv_batch_id,
       p_mms_membership.dv_load_date_time,
       p_mms_membership.dv_load_end_date_time
  from dbo.h_mms_membership
  join dbo.p_mms_membership
    on h_mms_membership.bk_hash = p_mms_membership.bk_hash  join #p_mms_membership_id_list
    on p_mms_membership.p_mms_membership_id = #p_mms_membership_id_list.p_mms_membership_id
   and p_mms_membership.bk_hash = #p_mms_membership_id_list.bk_hash
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
 where isnull(p_mms_membership.dv_greatest_satellite_date_time, convert(datetime, s_mms_membership.created_date_time, 102))!= case when p_mms_membership.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_membership.dv_load_end_date_time
              else p_mms_membership.dv_next_greatest_satellite_date_time
         end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_membership_history
       where d_mms_membership_history.p_mms_membership_id in (select p_mms_membership_id from #p_mms_membership_id_list)

      insert dbo.d_mms_membership_history(
                 bk_hash,
                 dim_mms_membership_key,
                 membership_id,
                 effective_date_time,
                 expiration_date_time,
                 advisor_employee_id,
                 club_id,
                 company_id,
                 created_date_time,
                 crm_opportunity_id,
                 current_price,
                 dim_crm_opportunity_key,
                 dim_mms_company_key,
                 dim_mms_membership_type_key,
                 dv_first_in_key_series,
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
                 updated_date_time,
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
             effective_date_time,
             expiration_date_time,
             advisor_employee_id,
             club_id,
             company_id,
             created_date_time,
             crm_opportunity_id,
             current_price,
             dim_crm_opportunity_key,
             dim_mms_company_key,
             dim_mms_membership_type_key,
             dv_first_in_key_series,
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
             updated_date_time,
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
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_history)
--Done!
end
