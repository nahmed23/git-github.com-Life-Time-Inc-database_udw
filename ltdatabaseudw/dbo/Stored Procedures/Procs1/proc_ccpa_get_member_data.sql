CREATE PROC [dbo].[proc_ccpa_get_member_data] @member_id [int] AS

declare @dim_mms_member_key varchar(33) = (select dim_mms_member_key from marketing.v_dim_mms_member where member_id = @member_id)
declare @dim_mms_membership_key varchar(32) = (select dim_mms_membership_key from marketing.v_dim_mms_member where member_id = @member_id)

if object_id('tempdb..#m') is not null drop table #m
create table #m with (distribution = round_robin) as
SELECT m.dim_mms_member_key
      ,m.member_id
      ,m.customer_name
      ,m.date_of_birth
      ,Datediff(yy,m.date_of_birth, GETDATE()) as AGE
      ,m.description_member
      ,m.email_address
      ,m.gender_abbreviation
      ,m.join_date
      ,m.member_active_flag
      ,m.membership_id
      ,ms.membership_activation_date
      ,ms.membership_address_city
      ,ms.membership_address_country
      ,ms.membership_address_line_1
      ,ms.membership_address_line_2
      ,ms.membership_address_postal_code
      ,ms.membership_address_state_abbreviation
      ,ms.membership_status
      ,ms.membership_type
	  ,CONCAT(mp_bus.area_code,mp_bus.number ) as business_phone_number
	  ,CONCAT(mp_home.area_code,mp_home.number ) as home_phone_number
	  ,[latest_body_ticket_item_date_time]
      ,[latest_hair_ticket_item_date_time]
      ,[latest_medi_ticket_item_date_time]
      ,[latest_nail_ticket_item_date_time]
      ,[latest_service_purchase_date_time]
      ,[latest_skin_ticket_item_date_time]
      ,[next_appointment_date_time]
      ,[next_body_appointment_date_time]
      ,[next_hair_appointment_date_time]
      ,[next_medi_appointment_date_time]
      ,[next_nail_appointment_date_time]
      ,[next_skin_appointment_date_time]
      ,[yearly_body_service_amount]
      ,[yearly_body_service_quantity]
      ,[yearly_hair_service_amount]
      ,[yearly_hair_service_quantity]
      ,[yearly_medi_service_amount]
      ,[yearly_medi_service_quantity]
      ,[yearly_nail_service_amount]
      ,[yearly_nail_service_quantity]
      ,[yearly_product_amount]
      ,[yearly_product_quantity]
      ,[yearly_service_amount]
      ,[yearly_service_quantity]
      ,[yearly_skin_service_amount]
      ,[yearly_skin_service_quantity]
from marketing.v_dim_mms_member m
join marketing.v_dim_mms_membership ms on m.dim_mms_membership_key = ms.dim_mms_membership_key
left join marketing.v_dim_mms_membership_phone mp_bus on ms.dim_mms_membership_key = mp_bus.dim_mms_membership_key and mp_bus.phone_type_dim_description_key = (select dim_description_key from marketing.v_dim_description where source_object = 'r_mms_val_phone_type' and description = 'business')
left join marketing.v_dim_mms_membership_phone mp_home on ms.dim_mms_membership_key = mp_home.dim_mms_membership_key and mp_home.phone_type_dim_description_key = (select dim_description_key from marketing.v_dim_description where source_object = 'r_mms_val_phone_type' and description = 'home')
left join [marketing].[v_fact_spabiz_member_summary]
on m.dim_mms_member_key = v_fact_spabiz_member_summary.dim_mms_member_key
where m.dim_mms_member_key = @dim_mms_member_key


/*-if only junior list is required then have "and description_member = 'Junior'" in where clause of @related_member_string----*/

declare @related_member_string varchar(max) = '"' +
(
select cast
(
string_agg(cast(description_member as nvarchar(max)) + ' - '+ cast(customer_name as nvarchar(255))
                  +', age: '+cast(datediff(yy,date_of_birth,getdate()) - CASE WHEN Month(date_of_birth) > Month(GetDate()) THEN 1
                                                                              WHEN Month(date_of_birth) = Month(GetDate()) AND Day(date_of_birth) > Day(GetDate()) THEN 1 
																			  ELSE 0 END as varchar(10))
                  +', gender: '+gender_abbreviation
				  +case when isnull(email_address,'') <> '' then ', email: '+email_address else '' end
			     ,char(13)) 
				 as nvarchar(4000)
				 )
  from marketing.v_dim_mms_member
 where dim_mms_membership_key = @dim_mms_membership_key 
 and dim_mms_member_key <> @dim_mms_member_key) +'"'


declare @cc_string varchar(max) = '"' +
(select cast(string_agg(cast(card_type as nvarchar (max)) + ' - '+ cast(credit_card_last_four_digits as varchar(20)) 
                  +', total: '+cast(cast(sum_amount as decimal(26,2)) as varchar)
				  +', last: '+cast(cast(max_tran_dt as date) as varchar(10))
			     ,char(13)) as nvarchar(4000)) 
  from (select top 100 card_type, credit_card_last_four_digits, sum(transaction_amount) sum_amount, max(transaction_date_time) max_tran_dt
          from marketing.v_fact_mms_pt_credit_card_transaction
         where dim_mms_member_key = @dim_mms_member_key
		 group by card_type, credit_card_last_four_digits
         order by max_tran_dt desc) x
) +'"'

declare @usage_string varchar(max) = '"' +
(select cast(string_agg(cast(club_name as varchar(max))+': '+cast(c as varchar(10))+', last: '+cast(max_dt as varchar(10)),char(13)) as nvarchar(4000))
  from (select v_dim_club.club_name, count(*) c, cast(max(checkin_date_time) as date) max_dt
          from marketing.v_fact_mms_member_usage
          join marketing.v_dim_club on v_fact_mms_member_usage.dim_club_key = v_dim_club.dim_club_key
         where checkin_dim_mms_member_key = @dim_mms_member_key
         group by v_dim_club.club_name ) x
)+'"'
declare @child_usage_string varchar(max) = '"' +
(select cast(string_agg(cast(customer_name as nvarchar(max)) + ', '+cast(club_name as nvarchar(255))+': '+cast(c as varchar(10))+', last: '+cast(max_dt as varchar(10)),char(13)) as nvarchar(4000))
  from (select v_dim_mms_member.customer_name,v_dim_club.club_name, count(*) c, cast(max(check_in_dim_date_key) as date) max_dt
          from marketing.v_fact_mms_child_center_usage
          join marketing.v_dim_club on v_fact_mms_child_center_usage.dim_club_key = v_dim_club.dim_club_key
          join marketing.v_dim_mms_member on v_fact_mms_child_center_usage.child_dim_mms_member_key = v_dim_mms_member.dim_mms_member_key
         where child_dim_mms_member_key in (select dim_mms_member_key 
                                              from marketing.v_dim_mms_member 
                                              where dim_mms_membership_key = @dim_mms_membership_key 
                                              and dim_mms_member_key <> @dim_mms_member_key)
         group by v_dim_mms_member.customer_name,v_dim_club.club_name 
         ) x
)+'"'

select *, @related_member_string related_member_list, @child_usage_string child_usage, @usage_string club_usage, @cc_string credit_card_transactions from #m

