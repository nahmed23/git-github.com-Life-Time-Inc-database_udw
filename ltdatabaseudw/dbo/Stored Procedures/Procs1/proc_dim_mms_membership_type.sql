CREATE PROC [dbo].[proc_dim_mms_membership_type] AS
begin

  set xact_abort on
  set nocount on

/* Multiple temp tables are needed for performance reasons associated with too many joins */

  if object_id('tempdb..#attribute') is not null drop table #attribute
  create table dbo.#attribute with (location = user_db, distribution = hash(membership_type_id)) as
  select l_mms_membership_type_attribute.membership_type_id,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 1 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_short_term_membership_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 2 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_express_membership_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 3 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_auto_ship_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 4 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_employee_membership_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 10 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_trade_out_membership_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 12 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_student_flex_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 13 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_26_and_under_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 15 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_vip_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 16 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_non_access_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 17 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_pending_non_access_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 18 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_junior_members_not_allowed_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 19 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_corporate_flex_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 20 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_not_eligible_for_magazine_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 21 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_free_magazine_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 23 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_my_health_check_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 24 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_flexible_pass_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 28 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_acquisition_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 32 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_founders_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 35 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_life_time_health_flag,
         case when sum(case when l_mms_membership_type_attribute.val_membership_type_attribute_id = 36 then 1 else 0 end) > 0 then 'Y' else 'N' end attribute_attrition_exclusion_flag
    from dbo.p_mms_membership_type_attribute
    join dbo.h_mms_membership_type_attribute
      on p_mms_membership_type_attribute.bk_hash = h_mms_membership_type_attribute.bk_hash
    join dbo.l_mms_membership_type_attribute
      on p_mms_membership_type_attribute.l_mms_membership_type_attribute_id = l_mms_membership_type_attribute.l_mms_membership_type_attribute_id
   where p_mms_membership_type_attribute.dv_load_end_date_time = 'dec 31, 9999'
     and h_mms_membership_type_attribute.dv_deleted = 0
   group by l_mms_membership_type_attribute.membership_type_id

  if object_id('tempdb..#product') is not null drop table #product
  create table dbo.#product with (location = user_db, distribution = hash(product_id)) as
  select p_mms_product.product_id,
         s_mms_product.description product_description,
         case when s_mms_product.product_id = 132
              then 'Y'
              else 'N' end product_house_account_flag,
         case when s_mms_product.product_id = 92
              then 'Y'
              else 'N' end product_investor_flag,
		 case when s_mms_product_2.access_by_price_paid_flag = 1
              then 'Y'
              else 'N' end access_by_price_paid_flag
    from dbo.p_mms_product
    join dbo.s_mms_product
	     on p_mms_product.s_mms_product_id = s_mms_product.s_mms_product_id
	left join dbo.s_mms_product_2
		 on p_mms_product.s_mms_product_2_id = s_mms_product_2.s_mms_product_2_id
   where p_mms_product.dv_load_end_date_time = 'dec 31, 9999'
   
   if object_id('tempdb..#membership_type_attribute') is not null drop table #membership_type_attribute
   create table dbo.#membership_type_attribute with (location = user_db, distribution = hash(membership_type_id)) as
   select l.membership_type_id membership_type_id,
          r.description description,
		  r.val_membership_type_attribute_id val_membership_type_attribute_id
   from p_mms_membership_type_attribute p
   join dbo.h_mms_membership_type_attribute h
     on p.bk_hash = h.bk_hash
   join dbo.l_mms_membership_type_attribute l
     on p.bk_hash = l.bk_hash
    and p.l_mms_membership_type_attribute_id = l.l_mms_membership_type_attribute_id
   join dbo.r_mms_val_membership_type_attribute r
     on l.val_membership_type_attribute_id = r.val_membership_type_attribute_id 
    and r.dv_load_end_date_time = 'dec 31, 9999'
  where h.dv_deleted = 0

  if object_id('tempdb..#membership_type') is not null drop table #membership_type
  create table dbo.#membership_type with (location = user_db, distribution = hash(membership_type_id)) as
  select p_mms_membership_type.bk_hash dim_mms_membership_type_key,
         p_mms_membership_type.membership_type_id membership_type_id,
         l_mms_membership_type.product_id,
         case when p_mms_membership_type.bk_hash in ('-997','-998','-999') then p_mms_membership_type.bk_hash
              when l_mms_membership_type.product_id is null then '-998'
              else 
	/*util_bk_hash[l_mms_membership_type.product_id,h_mms_product.product_id]*/
	convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership_type.product_id as varchar(500)),'z#@$k%&P'))),2)
          end dim_mms_product_key,
         case
              when s_mms_membership_type.assess_due_flag = 1 then 'Y'
              else 'N'
          end assess_dues_flag,
         case
              when s_mms_membership_type.short_term_membership_flag = 1 then 'Y'
              else 'N'
          end short_term_membership_flag,
         isnull(s_mms_membership_type.max_unit_type, 0) unit_type_maximum,
         case
              when s_mms_membership_type.express_membership_flag = 1 then 'Y'
              else 'N'
          end express_membership_flag,
         case when s_mms_membership_type.s_mms_membership_type_id < 0
              then ''
              else isnull(s_mms_membership_type.display_name, #product.product_description) end membership_type,
         case when s_mms_membership_type.s_mms_membership_type_id < 0
              then ''
              else #product.product_description end product_description,
         case when #product.product_house_account_flag = 'Y'
              then 'Y'
              else 'N' end product_house_account_flag,
         case when #product.product_investor_flag = 'Y'
              then 'Y'
              else 'N' end product_investor_flag,
		 #product.access_by_price_paid_flag,
         case
              when s_mms_membership_type.assess_jr_member_dues_flag = 0 then 'N'
              else 'Y'
          end assess_junior_member_dues_flag,
         case
              when s_mms_membership_type.waive_admin_fee_flag = 1 then 'Y'
              else 'N'
          end waive_admin_fee_flag,
         isnull(s_mms_membership_type.gta_sig_override, '') gta_signature_override,
         case
              when s_mms_membership_type.allow_partner_program_flag = 1 then 'Y'
              else 'N'
          end allow_partner_program_flag,
         isnull(s_mms_membership_type.min_unit_type, 0) unit_type_minimum,
         isnull(s_mms_membership_type.min_primary_age, 0) primary_age_minimum,
         case
              when s_mms_membership_type.waive_late_fee_flag = 1 then 'Y'
              else 'N'
          end waive_late_fee_flag,
         case
              when s_mms_membership_type.suppress_membership_card_flag = 1 then 'Y'
              else 'N'
          end suppress_membership_card_flag,
         case
              when s_mms_membership_type.waive_enrollment_fee_flag = 1 then 'Y'
              else 'N'
          end waive_enrollment_fee_flag,
  	     isnull(dssr_group.description, '') as attribute_dssr_group_description,
	       isnull(membership_status_summary_group.description, '') as attribute_membership_status_summary_group_description,
         l_mms_membership_type.val_membership_type_group_id val_membership_type_group_id,
         l_mms_membership_type.val_check_in_group_id val_check_in_group_id,
         l_mms_membership_type.val_membership_type_family_status_id val_membership_type_family_status_id,
         l_mms_membership_type.val_enrollment_type_id val_enrollment_type_id,
         l_mms_membership_type.val_unit_type_id val_unit_type_id,
         l_mms_membership_type.val_welcome_kit_type_id val_welcome_kit_type_id,
         l_mms_membership_type.val_pricing_method_id val_pricing_method_id,
         l_mms_membership_type.val_pricing_rule_id val_pricing_rule_id,
         l_mms_membership_type.val_restricted_group_id val_restricted_group_id,
         p_mms_membership_type.p_mms_membership_type_id,
         p_mms_membership_type.dv_load_date_time,
         p_mms_membership_type.dv_load_end_date_time,
         p_mms_membership_type.dv_batch_id
    from dbo.p_mms_membership_type
    join dbo.l_mms_membership_type
      on p_mms_membership_type.l_mms_membership_type_id = l_mms_membership_type.l_mms_membership_type_id
    join dbo.s_mms_membership_type
      on p_mms_membership_type.s_mms_membership_type_id = s_mms_membership_type.s_mms_membership_type_id
    left join #product
      on l_mms_membership_type.product_id = #product.product_id	  
	left join #membership_type_attribute dssr_group
	  on p_mms_membership_type.membership_type_id = dssr_group.membership_type_id
     and dssr_group.description like 'DSSR%'
	left join #membership_type_attribute membership_status_summary_group
	  on p_mms_membership_type.membership_type_id = membership_status_summary_group.membership_type_id
     and membership_status_summary_group.description like  'Membership Status Summary Group%'
   where p_mms_membership_type.dv_load_end_date_time = 'dec 31, 9999'

  if object_id('tempdb..#val1') is not null drop table #val1
  create table dbo.#val1 with (location = user_db, distribution = hash(membership_type_id)) as
  select #membership_type.dim_mms_membership_type_key,
         #membership_type.membership_type_id,
         isnull(r_mms_val_check_in_group.description, '') check_in_group_description,
         isnull(r_mms_val_enrollment_type.description, '') enrollment_type_description,
         isnull(replace(r_mms_val_membership_type_family_status.description, ' Membership Type', ''), '') family_status_description,
         isnull(r_mms_val_membership_type_group.description, '') membership_type_group_description
    from #membership_type
    left join dbo.r_mms_val_check_in_group
      on #membership_type.val_check_in_group_id = r_mms_val_check_in_group.val_check_in_group_id
      and r_mms_val_check_in_group.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_enrollment_type
      on #membership_type.val_enrollment_type_id = r_mms_val_enrollment_type.val_enrollment_type_id
      and r_mms_val_enrollment_type.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_membership_type_family_status
      on #membership_type.val_membership_type_family_status_id = r_mms_val_membership_type_family_status.val_membership_type_family_status_id
     and r_mms_val_membership_type_family_status.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_membership_type_group
      on #membership_type.val_membership_type_group_id = r_mms_val_membership_type_group.val_membership_type_group_id
     and r_mms_val_membership_type_group.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)

  if object_id('tempdb..#val2') is not null drop table #val2
  create table dbo.#val2 with (location = user_db, distribution = hash(membership_type_id)) as
  select #membership_type.dim_mms_membership_type_key,
         #membership_type.membership_type_id,
         isnull(r_mms_val_pricing_method.description, '') pricing_method_description,
         isnull(r_mms_val_pricing_rule.description, '') pricing_rule_description,
         isnull(r_mms_val_restricted_group.description, '') restricted_group_description,       
         isnull(r_mms_val_unit_type.description, '') unit_type_description,
         isnull(r_mms_val_welcome_kit_type.description, '') welcome_kit_type_description
    from #membership_type
    left join dbo.r_mms_val_pricing_method
      on #membership_type.val_pricing_method_id = r_mms_val_pricing_method.val_pricing_method_id
     and r_mms_val_pricing_method.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_pricing_rule
      on #membership_type.val_pricing_rule_id = r_mms_val_pricing_rule.val_pricing_rule_id
     and r_mms_val_pricing_rule.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_restricted_group
      on #membership_type.val_restricted_group_id = r_mms_val_restricted_group.val_restricted_group_id
     and r_mms_val_restricted_group.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_unit_type
      on #membership_type.val_unit_type_id = r_mms_val_unit_type.val_unit_type_id
     and r_mms_val_unit_type.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    left join dbo.r_mms_val_welcome_kit_type
      on #membership_type.val_welcome_kit_type_id = r_mms_val_welcome_kit_type.val_welcome_kit_type_id
     and r_mms_val_welcome_kit_type.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)

  if object_id('tempdb..#dim_mms_membership_type') is not null drop table #dim_mms_membership_type
  create table dbo.#dim_mms_membership_type with (location = user_db, distribution = hash(membership_type_id)) as
  select #membership_type.dim_mms_membership_type_key,
         #membership_type.membership_type_id,
         #membership_type.product_id,
         #membership_type.dim_mms_product_key,
         #membership_type.assess_dues_flag,
         #membership_type.short_term_membership_flag,
         #membership_type.unit_type_maximum,
         #membership_type.express_membership_flag,
         #membership_type.membership_type,
         #membership_type.product_description,
         #membership_type.product_house_account_flag,
         #membership_type.product_investor_flag,
		 #membership_type.access_by_price_paid_flag,
         #membership_type.assess_junior_member_dues_flag,
         #membership_type.waive_admin_fee_flag,
         #membership_type.gta_signature_override,
         #membership_type.allow_partner_program_flag,
         #membership_type.unit_type_minimum,
         #membership_type.primary_age_minimum,
         #membership_type.waive_late_fee_flag,
         #membership_type.suppress_membership_card_flag,
         #membership_type.waive_enrollment_fee_flag,
         #membership_type.attribute_dssr_group_description,
         #membership_type.attribute_membership_status_summary_group_description,
         #membership_type.val_membership_type_group_id,
         #membership_type.val_check_in_group_id,
         #membership_type.val_membership_type_family_status_id,
         #membership_type.val_enrollment_type_id,
         #membership_type.val_unit_type_id,
         #membership_type.val_welcome_kit_type_id,
         #membership_type.val_pricing_method_id,
         #membership_type.val_pricing_rule_id,
         #membership_type.val_restricted_group_id,
         #membership_type.p_mms_membership_type_id,
         #membership_type.dv_load_date_time,
         #membership_type.dv_load_end_date_time,
         #membership_type.dv_batch_id,
         #val1.check_in_group_description,
         #val1.enrollment_type_description,
         #val1.family_status_description,
         #val1.membership_type_group_description,
         #val2.pricing_method_description,
         #val2.pricing_rule_description,
         #val2.restricted_group_description,       
         #val2.unit_type_description,
         #val2.welcome_kit_type_description,
         isnull(#attribute.attribute_short_term_membership_flag, 'N') attribute_short_term_membership_flag,
         isnull(#attribute.attribute_express_membership_flag, 'N') attribute_express_membership_flag,
         isnull(#attribute.attribute_auto_ship_flag, 'N') attribute_auto_ship_flag,
         isnull(#attribute.attribute_employee_membership_flag, 'N') attribute_employee_membership_flag,
         isnull(#attribute.attribute_trade_out_membership_flag, 'N') attribute_trade_out_membership_flag,
         isnull(#attribute.attribute_student_flex_flag, 'N') attribute_student_flex_flag,
         isnull(#attribute.attribute_26_and_under_flag, 'N') attribute_26_and_under_flag,
         isnull(#attribute.attribute_vip_flag, 'N') attribute_vip_flag,
         isnull(#attribute.attribute_non_access_flag, 'N') attribute_non_access_flag,
         isnull(#attribute.attribute_pending_non_access_flag, 'N') attribute_pending_non_access_flag,
         isnull(#attribute.attribute_junior_members_not_allowed_flag, 'N') attribute_junior_members_not_allowed_flag,
         isnull(#attribute.attribute_corporate_flex_flag, 'N') attribute_corporate_flex_flag,
         isnull(#attribute.attribute_not_eligible_for_magazine_flag, 'N') attribute_not_eligible_for_magazine_flag,
         isnull(#attribute.attribute_free_magazine_flag, 'N') attribute_free_magazine_flag,
         isnull(#attribute.attribute_my_health_check_flag, 'N') attribute_my_health_check_flag,
         isnull(#attribute.attribute_flexible_pass_flag, 'N') attribute_flexible_pass_flag,
         isnull(#attribute.attribute_acquisition_flag, 'N') attribute_acquisition_flag,
         isnull(#attribute.attribute_founders_flag, 'N') attribute_founders_flag,
         isnull(#attribute.attribute_life_time_health_flag, 'N') attribute_life_time_health_flag,
         isnull(#attribute.attribute_attrition_exclusion_flag, 'N') attribute_attrition_exclusion_flag
    from #membership_type
    join #val1
      on #membership_type.dim_mms_membership_type_key = #val1.dim_mms_membership_type_key
    join #val2
      on #membership_type.dim_mms_membership_type_key = #val2.dim_mms_membership_type_key
    left join #attribute
      on #membership_type.membership_type_id = #attribute.membership_type_id


    truncate table dbo.dim_mms_membership_type
/* This is a full rebuild each time so the where clause is not needed*/
/*     where dim_mms_membership_type.dim_mms_membership_type_key in (select dim_mms_membership_type_key from #dim_mms_membership_type)*/

    insert dbo.dim_mms_membership_type
           (
            dim_mms_membership_type_key,
            membership_type_id,
            dim_mms_product_key,
            assess_dues_flag,
            short_term_membership_flag,
            unit_type_maximum,
            express_membership_flag,
            membership_type,
            product_id,
            product_description,
            product_house_account_flag,
            product_investor_flag,
			access_by_price_paid_flag,
            assess_junior_member_dues_flag,
            waive_admin_fee_flag,
            gta_signature_override,
            allow_partner_program_flag,
            unit_type_minimum,
            primary_age_minimum,
            waive_late_fee_flag,
            suppress_membership_card_flag,
            waive_enrollment_fee_flag,
            attribute_dssr_group_description,
            attribute_membership_status_summary_group_description,
            val_membership_type_group_id,
            val_check_in_group_id,
            val_membership_type_family_status_id,
            val_enrollment_type_id,
            val_unit_type_id,
            val_welcome_kit_type_id,
            val_pricing_method_id,
            val_pricing_rule_id,
            val_restricted_group_id,
            p_mms_membership_type_id,
            dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            check_in_group_description,
            enrollment_type_description,
            family_status_description,
            membership_type_group_description,
            pricing_method_description,
            pricing_rule_description,
            restricted_group_description,
            unit_type_description,
            welcome_kit_type_description,
            attribute_short_term_membership_flag,
            attribute_express_membership_flag,
            attribute_auto_ship_flag,
            attribute_employee_membership_flag,
            attribute_trade_out_membership_flag,
            attribute_student_flex_flag,
            attribute_26_and_under_flag,
            attribute_vip_flag,
            attribute_non_access_flag,
            attribute_pending_non_access_flag,
            attribute_junior_members_not_allowed_flag,
            attribute_corporate_flex_flag,
            attribute_not_eligible_for_magazine_flag,
            attribute_free_magazine_flag,
            attribute_my_health_check_flag,
            attribute_flexible_pass_flag,
            attribute_acquisition_flag,
            attribute_founders_flag,
            attribute_life_time_health_flag,
            attribute_attrition_exclusion_flag,
            dv_inserted_date_time,
            dv_insert_user
           )
    select dim_mms_membership_type_key,
           membership_type_id,
           dim_mms_product_key,
           assess_dues_flag,
           short_term_membership_flag,
           unit_type_maximum,
           express_membership_flag,
           membership_type,
           product_id,
           product_description,
           product_house_account_flag,
           product_investor_flag,
		   access_by_price_paid_flag,
           assess_junior_member_dues_flag,
           waive_admin_fee_flag,
           gta_signature_override,
           allow_partner_program_flag,
           unit_type_minimum,
           primary_age_minimum,
           waive_late_fee_flag,
           suppress_membership_card_flag,
           waive_enrollment_fee_flag,
           attribute_dssr_group_description,
           attribute_membership_status_summary_group_description,
           val_membership_type_group_id,
           val_check_in_group_id,
           val_membership_type_family_status_id,
           val_enrollment_type_id,
           val_unit_type_id,
           val_welcome_kit_type_id,
           val_pricing_method_id,
           val_pricing_rule_id,
           val_restricted_group_id,
           p_mms_membership_type_id,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           check_in_group_description,
           enrollment_type_description,
           family_status_description,
           membership_type_group_description,
           pricing_method_description,
           pricing_rule_description,
           restricted_group_description,
           unit_type_description,
           welcome_kit_type_description,
           attribute_short_term_membership_flag,
           attribute_express_membership_flag,
           attribute_auto_ship_flag,
           attribute_employee_membership_flag,
           attribute_trade_out_membership_flag,
           attribute_student_flex_flag,
           attribute_26_and_under_flag,
           attribute_vip_flag,
           attribute_non_access_flag,
           attribute_pending_non_access_flag,
           attribute_junior_members_not_allowed_flag,
           attribute_corporate_flex_flag,
           attribute_not_eligible_for_magazine_flag,
           attribute_free_magazine_flag,
           attribute_my_health_check_flag,
           attribute_flexible_pass_flag,
           attribute_acquisition_flag,
           attribute_founders_flag,
           attribute_life_time_health_flag,
           attribute_attrition_exclusion_flag,
           getdate(),
           suser_sname()
      from #dim_mms_membership_type

end
