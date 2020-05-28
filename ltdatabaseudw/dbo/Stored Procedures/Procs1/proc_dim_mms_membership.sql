CREATE PROC [dbo].[proc_dim_mms_membership] @current_dv_batch_id [bigint] AS
 begin
 
 set xact_abort on
 set nocount on
 
 /*Start!*/
/*-declare @current_dv_batch_id bigint = -1*/
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) dim_batch_id from dim_mms_membership)
 
 if object_id('tempdb..#Membership') is not null drop table #Membership
 create table dbo.#Membership with (distribution = hash(dim_mms_membership_key)) as
 
 with 
 membership (dim_mms_membership_key, membership_id, created_date_time, created_date_time_key, crm_opportunity_id, dim_crm_opportunity_key, current_price, dim_mms_company_key, membership_type_id, dim_mms_membership_type_key, 
             home_dim_club_key, membership_activation_date, membership_cancellation_request_date, membership_expiration_date, non_payment_termination_flag, original_sales_dim_employee_key, prior_plus_dim_mms_membership_type_key,   
             prior_plus_price, val_membership_source_id, val_membership_status_id, val_termination_reason_id, val_eft_option_id,advisor_employee_id, undiscounted_price, prior_plus_undiscounted_price,
			 p_mms_membership_id, dv_load_date_time,dv_load_end_date_time,dv_batch_id,include_batch_flag) as
 (
     select d_mms_membership.dim_mms_membership_key dim_mms_membership_key,
            d_mms_membership.membership_id,
            d_mms_membership.created_date_time,
            d_mms_membership.created_date_time_key,
            d_mms_membership.crm_opportunity_id,
            d_mms_membership.dim_crm_opportunity_key,
            d_mms_membership.current_price,
            d_mms_membership.dim_mms_company_key,
            d_mms_membership.membership_type_id,
            d_mms_membership.dim_mms_membership_type_key,
            d_mms_membership.home_dim_club_key,
            d_mms_membership.membership_activation_date,
            d_mms_membership.membership_cancellation_request_date,
            d_mms_membership.membership_expiration_date,
            d_mms_membership.non_payment_termination_flag,
            d_mms_membership.original_sales_dim_employee_key,     
            d_mms_membership.prior_plus_dim_membership_type_key prior_plus_dim_mms_membership_type_key,  
            d_mms_membership.prior_plus_price,     
            d_mms_membership.val_membership_source_id,
            d_mms_membership.val_membership_status_id,
            d_mms_membership.val_termination_reason_id,
            d_mms_membership.val_eft_option_id,
 		    d_mms_membership.advisor_employee_id,
			d_mms_membership.undiscounted_price,     /* Added for user story UDW-10242 */
			d_mms_membership.prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
            d_mms_membership.p_mms_membership_id,
            d_mms_membership.dv_load_date_time,
            d_mms_membership.dv_load_end_date_time,
            d_mms_membership.dv_batch_id,
            case when d_mms_membership.dv_batch_id > @max_dv_batch_id or d_mms_membership.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
       from d_mms_membership
 ),
 membership_address (dim_mms_membership_key,membership_address_city,membership_address_line_1,membership_address_line_2,membership_address_postal_code,
                     val_country_id,val_state_id,dv_load_date_time,dv_load_end_date_time,dv_batch_id, include_batch_flag) as
 (
     select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_membership_address.membership_id as varchar(500)),'z#@$k%&P'))),2) dim_mms_membership_key,
            isnull(s_mms_membership_address.city,'') membership_address_city,
            isnull(s_mms_membership_address.address_line_1,'') membership_address_line_1,
            isnull(s_mms_membership_address.address_line_2,'') membership_address_line_2,
            isnull(s_mms_membership_address.zip,'') membership_address_postal_code,
            l_mms_membership_address.val_country_id val_country_id,
            l_mms_membership_address.val_state_id val_state_id,
            p_mms_membership_address.dv_load_date_time,
            p_mms_membership_address.dv_load_end_date_time,
            p_mms_membership_address.dv_batch_id,
            case when p_mms_membership_address.dv_batch_id > @max_dv_batch_id or p_mms_membership_address.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
       from dbo.p_mms_membership_address
       join dbo.s_mms_membership_address
         on p_mms_membership_address.s_mms_membership_address_id = s_mms_membership_address.s_mms_membership_address_id
       join dbo.l_mms_membership_address
         on p_mms_membership_address.l_mms_membership_address_id = l_mms_membership_address.l_mms_membership_address_id    
      where p_mms_membership_address.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
 
 ),
 membership_type (dim_mms_membership_type_key, membership_type,attribute_dssr_group_description, attribute_membership_status_summary_group_description, 
                  dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
 (
     select dim_mms_membership_type.dim_mms_membership_type_key,
            dim_mms_membership_type.membership_type,
            dim_mms_membership_type.attribute_dssr_group_description,
            dim_mms_membership_type.attribute_membership_status_summary_group_description,
            dim_mms_membership_type.dv_load_date_time,
            dim_mms_membership_type.dv_load_end_date_time,
            dim_mms_membership_type.dv_batch_id,
            case when dim_mms_membership_type.dv_batch_id > @max_dv_batch_id or dim_mms_membership_type.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
       from dbo.dim_mms_membership_type
 ),
 primary_member (dim_mms_membership_key, join_date, join_date_key,dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
 (
     select d_mms_member.dim_mms_membership_key,
            d_mms_member.join_date,
            d_mms_member.join_date_key,
            d_mms_member.dv_load_date_time,
            d_mms_member.dv_load_end_date_time,
            d_mms_member.dv_batch_id,
            case when d_mms_member.dv_batch_id > @max_dv_batch_id or d_mms_member.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
       from dbo.d_mms_member
      where d_mms_member.val_member_type_id = 1 /*primary*/
 ),
 sales_promotion (dim_mms_membership_key, exclude_from_attrition_reporting_flag, val_revenue_reporting_category_id, val_sales_reporting_category_id,
                  dv_load_date_time, dv_load_end_date_time, dv_batch_id, include_batch_flag) as
 (
     select v_dim_mms_unique_membership_attribute.dim_mms_membership_key,
            d_mms_sales_promotion.exclude_from_attrition_reporting_flag,
            d_mms_sales_promotion.val_revenue_reporting_category_id,
            d_mms_sales_promotion.val_sales_reporting_category_id,
            d_mms_sales_promotion.dv_load_date_time,
            d_mms_sales_promotion.dv_load_end_date_time,
            d_mms_sales_promotion.dv_batch_id,
            case when d_mms_sales_promotion.dv_batch_id > @max_dv_batch_id or d_mms_sales_promotion.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
       from dbo.v_dim_mms_unique_membership_attribute
       left join dbo.d_mms_sales_promotion
         on v_dim_mms_unique_membership_attribute.membership_attribute_value = d_mms_sales_promotion.sales_promotion_id
      where v_dim_mms_unique_membership_attribute.val_membership_attribute_type_id = 3 /*/*changed the column_name ref_val_membership_attribute_type_id to val_membership_attribute_type_id for UDW-10675*/*/
 ),
 
 /* Added for user story UDW-11816 */
 
 sales_transaction_item ( dim_mms_membership_key, sales_channel_dim_description_key, dv_load_date_time, 
						dv_load_end_date_time, dv_batch_id, include_batch_flag ) as
 (	select 	fact_mms_sales_transaction_item.dim_mms_membership_key, 
			fact_mms_sales_transaction_item.sales_channel_dim_description_key, 
			fact_mms_sales_transaction_item.dv_load_date_time, 
			fact_mms_sales_transaction_item.dv_load_end_date_time, 
			fact_mms_sales_transaction_item.dv_batch_id,
			case when fact_mms_sales_transaction_item.dv_batch_id > @max_dv_batch_id or fact_mms_sales_transaction_item.dv_batch_id = @current_dv_batch_id then 1 else 0 end include_batch_flag
		from dbo.fact_mms_sales_transaction_item
		where dim_mms_product_key = (select dim_mms_product_key from dbo.dim_mms_product where product_id = 88)
			and fact_mms_sales_transaction_item.active_transaction_flag = 'Y'
			and fact_mms_sales_transaction_item.membership_charge_flag = 'Y'
)
 
 select membership.dim_mms_membership_key dim_mms_membership_key,
        membership.membership_id,
        isnull(membership.created_date_time, primary_member.join_date) created_date_time,
        isnull(membership.created_date_time_key, primary_member.join_date_key) created_date_time_key,
        membership.crm_opportunity_id,
        membership.dim_crm_opportunity_key,
        membership.current_price,
        membership.dim_mms_company_key dim_mms_company_key,
        membership.membership_type_id,
        membership.dim_mms_membership_type_key,
        membership.home_dim_club_key home_dim_club_key,
        membership.membership_activation_date,
        membership_address.membership_address_city,
        membership_address.membership_address_line_1,
        membership_address.membership_address_line_2,
        membership_address.membership_address_postal_code,
        membership.membership_cancellation_request_date,
        membership.membership_expiration_date,
        membership_type.membership_type,
        case when membership.val_termination_reason_id in (21, 41, 42, 59) and membership_type.attribute_dssr_group_description != 'DSSR_Other' then 'Y'
            else 'N'
        end money_back_cancellation_flag,
        membership.non_payment_termination_flag,
        membership.original_sales_dim_employee_key,     
        membership.prior_plus_dim_mms_membership_type_key,  
        prior_plus_membership_type.membership_type prior_plus_membership_type,
        membership.prior_plus_price,     
               
        membership_type.attribute_dssr_group_description,
        membership_type.attribute_membership_status_summary_group_description,
        sales_promotion.exclude_from_attrition_reporting_flag,
        sales_promotion.val_revenue_reporting_category_id,
        sales_promotion.val_sales_reporting_category_id,
 
        membership.val_membership_source_id,
        membership.val_membership_status_id,
        membership.val_termination_reason_id,
        membership_address.val_country_id,
        membership.val_eft_option_id,
 	    membership.advisor_employee_id,
		membership.undiscounted_price,     /* Added for user story UDW-10242 */
		membership.prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
        membership_address.val_state_id,
        membership.p_mms_membership_id,
		sales_transaction_item.sales_channel_dim_description_key, /* Added for user story UDW-11816 */
		case when membership.dv_batch_id >= isnull(membership_address.dv_batch_id,-1)
               and membership.dv_batch_id >= isnull(prior_plus_membership_type.dv_batch_id,-1)
               and membership.dv_batch_id >= isnull(membership_type.dv_batch_id,-1)
               and membership.dv_batch_id >= isnull(primary_member.dv_batch_id,-1)
               and membership.dv_batch_id >= isnull(sales_promotion.dv_batch_id,-1)
			   and membership.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
              then membership.dv_batch_id
              when membership_address.dv_batch_id >= isnull(prior_plus_membership_type.dv_batch_id,-1)
               and membership_address.dv_batch_id >= isnull(membership_type.dv_batch_id,-1)
               and membership_address.dv_batch_id >= isnull(primary_member.dv_batch_id,-1)
               and membership_address.dv_batch_id >= isnull(sales_promotion.dv_batch_id,-1)
			   and membership_address.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
              then membership_address.dv_batch_id
              when prior_plus_membership_type.dv_batch_id >= isnull(membership_type.dv_batch_id,-1)
               and prior_plus_membership_type.dv_batch_id >= isnull(primary_member.dv_batch_id,-1)
               and prior_plus_membership_type.dv_batch_id >= isnull(sales_promotion.dv_batch_id,-1)
			   and prior_plus_membership_type.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
              then prior_plus_membership_type.dv_batch_id
              when membership_type.dv_batch_id >= isnull(primary_member.dv_batch_id,-1)
               and membership_type.dv_batch_id >= isnull(sales_promotion.dv_batch_id,-1)
			   and membership_type.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
              then membership_type.dv_batch_id
              when primary_member.dv_batch_id >= isnull(sales_promotion.dv_batch_id,-1)
			   and primary_member.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
              then primary_member.dv_batch_id
			  when sales_promotion.dv_batch_id >= isnull(sales_transaction_item.dv_batch_id,-1)
			  then sales_promotion.dv_batch_id
			  else isnull(sales_transaction_item.dv_batch_id,-1) 
          end dv_batch_id,
 
         case when membership.dv_load_date_time >= isnull(membership_address.dv_load_date_time,'Jan 1, 1753')
               and membership.dv_load_date_time >= isnull(prior_plus_membership_type.dv_load_date_time,'Jan 1, 1753')
               and membership.dv_load_date_time >= isnull(membership_type.dv_load_date_time,'Jan 1, 1753')
               and membership.dv_load_date_time >= isnull(primary_member.dv_load_date_time,'Jan 1, 1753')
               and membership.dv_load_date_time >= isnull(sales_promotion.dv_load_date_time,'Jan 1, 1753')
			   and membership.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
              then membership.dv_load_date_time
              when membership_address.dv_load_date_time >= isnull(prior_plus_membership_type.dv_load_date_time,'Jan 1, 1753')
               and membership_address.dv_load_date_time >= isnull(membership_type.dv_load_date_time,'Jan 1, 1753')
               and membership_address.dv_load_date_time >= isnull(primary_member.dv_load_date_time,'Jan 1, 1753')
               and membership_address.dv_load_date_time >= isnull(sales_promotion.dv_load_date_time,'Jan 1, 1753')
			   and membership_address.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
              then membership_address.dv_load_date_time
              when prior_plus_membership_type.dv_load_date_time >= isnull(membership_type.dv_load_date_time,'Jan 1, 1753')
               and prior_plus_membership_type.dv_load_date_time >= isnull(primary_member.dv_load_date_time,'Jan 1, 1753')
               and prior_plus_membership_type.dv_load_date_time >= isnull(sales_promotion.dv_load_date_time,'Jan 1, 1753')
			   and prior_plus_membership_type.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
              then prior_plus_membership_type.dv_load_date_time
              when membership_type.dv_load_date_time >= isnull(primary_member.dv_load_date_time,'Jan 1, 1753')
               and membership_type.dv_load_date_time >= isnull(sales_promotion.dv_load_date_time,'Jan 1, 1753')
			   and membership_type.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
              then membership_type.dv_load_date_time
              when primary_member.dv_load_date_time >= isnull(sales_promotion.dv_load_date_time,'Jan 1, 1753')
			   and primary_member.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
              then primary_member.dv_load_date_time
			  when sales_promotion.dv_load_date_time >= isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753')
			  then sales_promotion.dv_load_date_time
			  else isnull(sales_transaction_item.dv_load_date_time,'Jan 1, 1753') 
          end dv_load_date_time,
 
         case when membership.dv_load_end_date_time >= isnull(membership_address.dv_load_end_date_time,'Jan 1, 1753')
               and membership.dv_load_end_date_time >= isnull(prior_plus_membership_type.dv_load_end_date_time,'Jan 1, 1753')
               and membership.dv_load_end_date_time >= isnull(membership_type.dv_load_end_date_time,'Jan 1, 1753')
               and membership.dv_load_end_date_time >= isnull(primary_member.dv_load_end_date_time,'Jan 1, 1753')
               and membership.dv_load_end_date_time >= isnull(sales_promotion.dv_load_end_date_time,'Jan 1, 1753')
			   and membership.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
              then membership.dv_load_end_date_time
              when membership_address.dv_load_end_date_time >= isnull(prior_plus_membership_type.dv_load_end_date_time,'Jan 1, 1753')
               and membership_address.dv_load_end_date_time >= isnull(membership_type.dv_load_end_date_time,'Jan 1, 1753')
               and membership_address.dv_load_end_date_time >= isnull(primary_member.dv_load_end_date_time,'Jan 1, 1753')
               and membership_address.dv_load_end_date_time >= isnull(sales_promotion.dv_load_end_date_time,'Jan 1, 1753')
			   and membership_address.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
              then membership_address.dv_load_end_date_time
              when prior_plus_membership_type.dv_load_end_date_time >= isnull(membership_type.dv_load_end_date_time,'Jan 1, 1753')
               and prior_plus_membership_type.dv_load_end_date_time >= isnull(primary_member.dv_load_end_date_time,'Jan 1, 1753')
               and prior_plus_membership_type.dv_load_end_date_time >= isnull(sales_promotion.dv_load_end_date_time,'Jan 1, 1753')
			   and prior_plus_membership_type.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
              then prior_plus_membership_type.dv_load_end_date_time
              when membership_type.dv_load_end_date_time >= isnull(primary_member.dv_load_end_date_time,'Jan 1, 1753')
               and membership_type.dv_load_end_date_time >= isnull(sales_promotion.dv_load_end_date_time,'Jan 1, 1753')
			   and membership_type.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
              then membership_type.dv_load_end_date_time
              when primary_member.dv_load_end_date_time >= isnull(sales_promotion.dv_load_end_date_time,'Jan 1, 1753')
			   and primary_member.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
              then primary_member.dv_load_end_date_time
			  when sales_promotion.dv_load_end_date_time >= isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
			  then sales_promotion.dv_load_end_date_time
			  else isnull(sales_transaction_item.dv_load_end_date_time,'Jan 1, 1753')
		  end dv_load_end_date_time
   from membership
   left join membership_address
     on membership.dim_mms_membership_key = membership_address.dim_mms_membership_key
   left join membership_type
     on membership.dim_mms_membership_type_key = membership_type.dim_mms_membership_type_key
   left join membership_type prior_plus_membership_type
     on membership.prior_plus_dim_mms_membership_type_key = prior_plus_membership_type.dim_mms_membership_type_key
   left join primary_member
     on membership.dim_mms_membership_key = primary_member.dim_mms_membership_key
   left join sales_promotion
     on membership.dim_mms_membership_key = sales_promotion.dim_mms_membership_key
   left join sales_transaction_item
     on membership.dim_mms_membership_key = sales_transaction_item.dim_mms_membership_key
  where membership.include_batch_flag = 1
     or isnull(membership_address.include_batch_flag,0) = 1
     or isnull(membership_type.include_batch_flag,0) = 1
     or isnull(prior_plus_membership_type.include_batch_flag,0) = 1
     or isnull(primary_member.include_batch_flag,0) = 1
     or isnull(sales_promotion.include_batch_flag,0) = 1
	 or isnull(sales_transaction_item.include_batch_flag,0) = 1

 begin tran
   delete dbo.dim_mms_membership
    where dim_mms_membership.dim_mms_membership_key in (select dim_mms_membership_key from #Membership)
 
   insert dim_mms_membership
   (
          dim_mms_membership_key,
          membership_id,
          attrition_date,
          created_date_time,
          created_date_time_key,
          crm_opportunity_id,
          current_price,      
          dim_crm_opportunity_key, 
          dim_mms_company_key,
          dim_mms_membership_type_key,
          eft_option,
          home_dim_club_key,
          membership_activation_date,
          membership_address_city,
          membership_address_country,
          membership_address_line_1,
          membership_address_line_2,
          membership_address_postal_code,
          membership_address_state_abbreviation,
          membership_cancellation_request_date,
          membership_expiration_date,
          membership_source,
          membership_status,
          membership_type,
          membership_type_id,
          money_back_cancellation_flag,
          non_payment_termination_flag,
          original_sales_dim_team_member_key,
          prior_plus_membership_type_key,
          prior_plus_membership_type,
          prior_plus_price,
          revenue_reporting_category_description,
          sales_reporting_category_description,
          termination_reason,
          val_country_id,
          val_eft_option_id,
 		  advisor_employee_id,
		  undiscounted_price,     /* Added for user story UDW-10242 */
		  prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
          val_membership_source_id,
          val_membership_status_id,
          val_revenue_reporting_category_id,
          val_sales_reporting_category_id,
          val_state_id,
          val_termination_reason_id,
          p_mms_membership_id,
		  membership_sales_channel_dim_description_key,  /* Added for user story UDW-11816 */
          dv_load_date_time,
          dv_load_end_date_time,
          dv_batch_id,
          dv_inserted_date_time,
          dv_insert_user 
		  )

   select #Membership.dim_mms_membership_key,
          #Membership.membership_id,
          case when money_back_cancellation_flag = 'Y' then convert(datetime, '9999.12.31', 102)
               when attribute_dssr_group_description = 'Y' then convert(datetime, '9999.12.31', 102)
               when attribute_membership_status_summary_group_description is null then convert(datetime, '9999.12.31', 102)
               when attribute_membership_status_summary_group_description not in ('Membership Status Summary Group 2 Revenue',
                                                                                  'Membership Status Summary Group 3 Revenue LTHealth',
                                                                                  'Membership Status Summary Group 2 Revenue - 1 Member',
                                                                                  'Membership Status Summary Group 3 Revenue - 2 Members',
                                                                                  'Membership Status Summary Group 4 Revenue - 3/3+ Members',
                                                                                  'Membership Status Summary Group 5 Revenue - 4+ Members',
                                                                                  'Membership Status Summary Group 9 Rev Student Flex',
                                                                                  'Membership Status Summary Group 10 Rev Student Access',
                                                                                  'Membership Status Summary Group 6 Revenue On-Hold & Non-Access') then convert(datetime, '9999.12.31', 102)
               else membership_expiration_date 
          end attrition_date,
          #Membership.created_date_time,
          #Membership.created_date_time_key,
          #Membership.crm_opportunity_id,
          #Membership.current_price,
          #Membership.dim_crm_opportunity_key,
          #Membership.dim_mms_company_key,
          #Membership.dim_mms_membership_type_key,
          r_mms_val_eft_option.description,/*eft_option,*/
          #Membership.home_dim_club_key,
          #Membership.membership_activation_date,
          #Membership.membership_address_city,
          r_mms_val_country.description,/*#Membership.membership_address_country,*/
          #Membership.membership_address_line_1,
          #Membership.membership_address_line_2,
          #Membership.membership_address_postal_code,
          r_mms_val_state.abbreviation,/*membership_address_state_abbreviation,*/
          #Membership.membership_cancellation_request_date,
          #Membership.membership_expiration_date,
          r_mms_val_membership_source.description,/*membership_source,*/
          r_mms_val_membership_status.description,/*membership_status,*/
          #Membership.membership_type,
          #Membership.membership_type_id,
          #Membership.money_back_cancellation_flag,
          #Membership.non_payment_termination_flag,
          #Membership.original_sales_dim_employee_key,
          #Membership.prior_plus_dim_mms_membership_type_key,
          #Membership.prior_plus_membership_type,
          #Membership.prior_plus_price,
          case when r_mms_val_revenue_reporting_category.description is null then isnull(attribute_membership_status_summary_group_description, '')
               else r_mms_val_revenue_reporting_category.description 
          end revenue_reporting_category_description,
          case when r_mms_val_sales_reporting_category.description is null then isnull(attribute_dssr_group_description , '')
               else r_mms_val_sales_reporting_category.description
          end sales_reporting_category_description,
          r_mms_val_termination_reason.description,/*termination_reason,*/
          #Membership.val_country_id,
          #Membership.val_eft_option_id,
 		  #Membership.advisor_employee_id,
		  #Membership.undiscounted_price,     /* Added for user story UDW-10242 */
		  #Membership.prior_plus_undiscounted_price,  /* Added for user story UDW-10242 */
          #Membership.val_membership_source_id,
          #Membership.val_membership_status_id,
          #Membership.val_revenue_reporting_category_id,
          #Membership.val_sales_reporting_category_id,
          #Membership.val_state_id,
          #Membership.val_termination_reason_id,
          #Membership.p_mms_membership_id,
		  /*-#Membership.sales_channel_dim_description_key,  /* Added for user story UDW-11816 */*/
		  case when #Membership.val_membership_source_id = 6 then 'mms_sales_channel_special_employee_-4' 
			   when #Membership.sales_channel_dim_description_key is not null then #Membership.sales_channel_dim_description_key
			   else 'mms_sales_channel_mms_default' end membership_sales_channel_dim_description_key, /* Added for user story UDW-11816 */
		  #Membership.dv_load_date_time,
          #Membership.dv_load_end_date_time,
          #Membership.dv_batch_id,
          getdate(),
          suser_sname()
   from #Membership
   left join dbo.r_mms_val_state
     on #Membership.val_state_id = r_mms_val_state.val_state_id
    and r_mms_val_state.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_termination_reason
     on #Membership.val_termination_reason_id = r_mms_val_termination_reason.val_termination_reason_id
    and r_mms_val_termination_reason.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_country
     on #Membership.val_country_id = r_mms_val_country.val_country_id
    and r_mms_val_country.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_eft_option
     on #Membership.val_eft_option_id = r_mms_val_eft_option.val_eft_option_id
    and r_mms_val_eft_option.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_membership_status
     on #Membership.val_membership_status_id = r_mms_val_membership_status.val_membership_status_id
    and r_mms_val_membership_status.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_membership_source
     on #Membership.val_membership_source_id = r_mms_val_membership_source.val_membership_source_id
    and r_mms_val_membership_source.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_revenue_reporting_category
     on #Membership.val_revenue_reporting_category_id = r_mms_val_revenue_reporting_category.val_revenue_reporting_category_id
    and r_mms_val_revenue_reporting_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
   left join dbo.r_mms_val_sales_reporting_category
     on #Membership.val_sales_reporting_category_id = r_mms_val_sales_reporting_category.val_sales_reporting_category_id
    and r_mms_val_sales_reporting_category.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)
    
commit tran

   declare @force_replicate bigint = (select max(dv_batch_id) from dim_mms_membership)
 
 end
