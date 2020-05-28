CREATE PROC [dbo].[proc_d_mms_membership_type_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_type_history);

if object_id('tempdb..#p_mms_membership_type_id_list') is not null drop table #p_mms_membership_type_id_list
create table dbo.#p_mms_membership_type_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_membership_type_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_membership_type_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_membership_type_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_membership_type_id,bk_hash) as
(
    select d_mms_membership_type_history.p_mms_membership_type_id,
           d_mms_membership_type_history.bk_hash
      from dbo.d_mms_membership_type_history
      join undo_delete
        on d_mms_membership_type_history.bk_hash = undo_delete.bk_hash
       and d_mms_membership_type_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_membership_type_insert (p_mms_membership_type_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_membership_type_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_membership_type
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_membership_type_update (p_mms_membership_type_id,bk_hash) as
(
    select p_mms_membership_type.p_mms_membership_type_id,
           p_mms_membership_type.bk_hash
      from dbo.p_mms_membership_type
      join p_mms_membership_type_insert
        on p_mms_membership_type.bk_hash = p_mms_membership_type_insert.bk_hash
       and p_mms_membership_type.dv_load_end_date_time = p_mms_membership_type_insert.dv_load_date_time
)
select undo_delete.p_mms_membership_type_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_membership_type_id,
       bk_hash
  from undo_update
union
select p_mms_membership_type_insert.p_mms_membership_type_id,
       bk_hash
  from p_mms_membership_type_insert
union
select p_mms_membership_type_update.p_mms_membership_type_id,
       bk_hash
  from p_mms_membership_type_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_membership_type_id_list.bk_hash,
       p_mms_membership_type.bk_hash dim_mms_membership_type_key,
       p_mms_membership_type.membership_type_id membership_type_id,
       isnull(p_mms_membership_type.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_mms_membership_type.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
        then p_mms_membership_type.dv_load_end_date_time     
        else p_mms_membership_type.dv_next_greatest_satellite_date_time    end expiration_date_time,
       case when s_mms_membership_type.allow_partner_program_flag = 1 then 'Y' else 'N' end allow_partner_program_flag,
       case when s_mms_membership_type.assess_due_flag = 1 then 'Y' else 'N' end assess_dues_flag,
       case when ISNULL(s_mms_membership_type.assess_jr_member_dues_flag, 1) = 1 then 'Y' else 'N' end assess_junior_member_dues_flag,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash  
       when l_mms_membership_type.val_check_in_group_id is null then '-998' 
       else 'r_mms_val_check_in_group'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_check_in_group_id as int) as varchar(500)),'z#@$k%&P'))),2)  end check_in_group_dim_description_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.product_id as int) as varchar(500)),'z#@$k%&P'))),2) dim_mms_product_key,
       s_mms_membership_type.display_name display_name,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_enrollment_type_id is null then '-998'
        else 'r_mms_val_enrollment_type'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_enrollment_type_id as int) as varchar(500)),'z#@$k%&P'))),2)  end enrollment_type_dim_description_key,
       case when s_mms_membership_type.express_membership_flag = 1 then 'Y' else 'N' end express_membership_flag,
       isnull(s_mms_membership_type.gta_sig_override, '') gta_signature_override,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_membership_type_family_status_id is null then '-998' else 'r_mms_val_membership_type_family_status'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_membership_type_family_status_id as int) as varchar(500)),'z#@$k%&P'))),2)  end membership_type_family_status_dim_description_key,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_membership_type_group_id is null then '-998' else 'r_mms_val_membership_type_group'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_membership_type_group_id as int) as varchar(500)),'z#@$k%&P'))),2)  end membership_type_group_dim_description_key,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash  
       when l_mms_membership_type.val_pricing_method_id is null then '-998' 
       else 'r_mms_val_pricing_method'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_pricing_method_id as int) as varchar(500)),'z#@$k%&P'))),2)  end pricing_method_dim_description_key,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_pricing_rule_id is null then '-998' else 
        'r_mms_val_pricing_rule'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_pricing_rule_id as int) as varchar(500)),'z#@$k%&P'))),2)  end pricing_rule_dim_description_key,
       isnull(s_mms_membership_type.min_primary_age,0) primary_age_minimum,
       l_mms_membership_type.product_id product_id,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_restricted_group_id is null then '-998' else
        'r_mms_val_restricted_group'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_restricted_group_id as int) as varchar(500)),'z#@$k%&P'))),2)  end restricted_group_dim_description_key,
       case when s_mms_membership_type.short_term_membership_flag = 1 then 'Y' else 'N' end short_term_membership_flag,
       case when s_mms_membership_type.suppress_membership_card_flag = 1 then 'Y' else 'N' end suppress_membership_card_flag,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash  
       when l_mms_membership_type.val_unit_type_id is null then '-998' else 'r_mms_val_unit_type'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_unit_type_id as int) as varchar(500)),'z#@$k%&P'))),2)  end unit_type_dim_description_key,
       isnull(s_mms_membership_type.max_unit_type,0) unit_type_maximum,
       isnull(s_mms_membership_type.min_unit_type,0) unit_type_minimum,
       l_mms_membership_type.val_check_in_group_id val_check_in_group_id,
       l_mms_membership_type.val_enrollment_type_id val_enrollment_type_id,
       l_mms_membership_type.val_membership_type_family_status_id val_membership_type_family_status_id,
       l_mms_membership_type.val_membership_type_group_id val_membership_type_group_id,
       l_mms_membership_type.val_pricing_method_id val_pricing_method_id,
       l_mms_membership_type.val_pricing_rule_id val_pricing_rule_id,
       l_mms_membership_type.val_restricted_group_id val_restricted_group_id,
       l_mms_membership_type.val_unit_type_id val_unit_type_id,
       l_mms_membership_type.val_welcome_kit_type_id val_welcome_kit_type_id,
       case when s_mms_membership_type.waive_admin_fee_flag = 1 then 'Y' else 'N' end waive_admin_fee_flag,
       case when s_mms_membership_type.waive_late_fee_flag = 1 then 'Y' else 'N' end waive_late_fee_flag,
       case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash 
        when l_mms_membership_type.val_welcome_kit_type_id is null then '-998' else 'r_mms_val_welcome_kit_type'+'_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_type.val_welcome_kit_type_id as int) as varchar(500)),'z#@$k%&P'))),2)  end welcome_kit_type_dim_description_key,
       h_mms_membership_type.dv_deleted,
       p_mms_membership_type.p_mms_membership_type_id,
       p_mms_membership_type.dv_batch_id,
       p_mms_membership_type.dv_load_date_time,
       p_mms_membership_type.dv_load_end_date_time
  from dbo.h_mms_membership_type
  join dbo.p_mms_membership_type
    on h_mms_membership_type.bk_hash = p_mms_membership_type.bk_hash  join #p_mms_membership_type_id_list
    on p_mms_membership_type.p_mms_membership_type_id = #p_mms_membership_type_id_list.p_mms_membership_type_id
   and p_mms_membership_type.bk_hash = #p_mms_membership_type_id_list.bk_hash
  join dbo.l_mms_membership_type
    on p_mms_membership_type.bk_hash = l_mms_membership_type.bk_hash
   and p_mms_membership_type.l_mms_membership_type_id = l_mms_membership_type.l_mms_membership_type_id
  join dbo.s_mms_membership_type
    on p_mms_membership_type.bk_hash = s_mms_membership_type.bk_hash
   and p_mms_membership_type.s_mms_membership_type_id = s_mms_membership_type.s_mms_membership_type_id
 where isnull(p_mms_membership_type.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mms_membership_type.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
 then p_mms_membership_type.dv_load_end_date_time     
 else p_mms_membership_type.dv_next_greatest_satellite_date_time    end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_membership_type_history
       where d_mms_membership_type_history.p_mms_membership_type_id in (select p_mms_membership_type_id from #p_mms_membership_type_id_list)

      insert dbo.d_mms_membership_type_history(
                 bk_hash,
                 dim_mms_membership_type_key,
                 membership_type_id,
                 effective_date_time,
                 expiration_date_time,
                 allow_partner_program_flag,
                 assess_dues_flag,
                 assess_junior_member_dues_flag,
                 check_in_group_dim_description_key,
                 dim_mms_product_key,
                 display_name,
                 enrollment_type_dim_description_key,
                 express_membership_flag,
                 gta_signature_override,
                 membership_type_family_status_dim_description_key,
                 membership_type_group_dim_description_key,
                 pricing_method_dim_description_key,
                 pricing_rule_dim_description_key,
                 primary_age_minimum,
                 product_id,
                 restricted_group_dim_description_key,
                 short_term_membership_flag,
                 suppress_membership_card_flag,
                 unit_type_dim_description_key,
                 unit_type_maximum,
                 unit_type_minimum,
                 val_check_in_group_id,
                 val_enrollment_type_id,
                 val_membership_type_family_status_id,
                 val_membership_type_group_id,
                 val_pricing_method_id,
                 val_pricing_rule_id,
                 val_restricted_group_id,
                 val_unit_type_id,
                 val_welcome_kit_type_id,
                 waive_admin_fee_flag,
                 waive_late_fee_flag,
                 welcome_kit_type_dim_description_key,
                 deleted_flag,
                 p_mms_membership_type_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_mms_membership_type_key,
             membership_type_id,
             effective_date_time,
             expiration_date_time,
             allow_partner_program_flag,
             assess_dues_flag,
             assess_junior_member_dues_flag,
             check_in_group_dim_description_key,
             dim_mms_product_key,
             display_name,
             enrollment_type_dim_description_key,
             express_membership_flag,
             gta_signature_override,
             membership_type_family_status_dim_description_key,
             membership_type_group_dim_description_key,
             pricing_method_dim_description_key,
             pricing_rule_dim_description_key,
             primary_age_minimum,
             product_id,
             restricted_group_dim_description_key,
             short_term_membership_flag,
             suppress_membership_card_flag,
             unit_type_dim_description_key,
             unit_type_maximum,
             unit_type_minimum,
             val_check_in_group_id,
             val_enrollment_type_id,
             val_membership_type_family_status_id,
             val_membership_type_group_id,
             val_pricing_method_id,
             val_pricing_rule_id,
             val_restricted_group_id,
             val_unit_type_id,
             val_welcome_kit_type_id,
             waive_admin_fee_flag,
             waive_late_fee_flag,
             welcome_kit_type_dim_description_key,
             dv_deleted,
             p_mms_membership_type_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_type_history)
--Done!
end
