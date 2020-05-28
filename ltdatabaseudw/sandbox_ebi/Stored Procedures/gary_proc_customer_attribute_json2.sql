CREATE PROC [sandbox_ebi].[gary_proc_customer_attribute_json2] @dv_batch_id [bigint],@start [int],@end [int] AS
begin

  set nocount on
  set xact_abort on

  declare @row_start int = @start
  declare @row_end int = @end

  -- Get the customer attributes
  -- replace any special characters in JSON with escape sequences
  if object_id('tempdb..#wrk_pega_customer_attribute') is not null drop table #wrk_pega_customer_attribute
  create table dbo.#wrk_pega_customer_attribute with (distribution = hash (dim_mms_member_key),location = user_db) as
  select dim_mms_member_key,
         sequence_number,
         membership_id,
         member_id,
         entity_id,
         replace(replace(replace(replace(replace(replace(first_name,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') first_name,
         replace(replace(replace(replace(replace(replace(last_name,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') last_name,
         date_of_birth,
         member_active_flag,
         member_gender,
         assess_junior_member_dues_flag,
         member_type,
         member_join_date,
         party_id,
         member_mms_home_club_id,
         membership_created_date_time,
         membership_phone,
         replace(replace(replace(replace(replace(replace(membership_address_line_1,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') membership_address_line_1,
         replace(replace(replace(replace(replace(replace(membership_address_line_2,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') membership_address_line_2,
         replace(replace(replace(replace(replace(replace(membership_address_city,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') membership_address_city,
         membership_address_country,
         replace(replace(replace(replace(replace(replace(membership_address_postal_code,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') membership_address_postal_code,
         membership_address_state_abbreviation,
         membership_cancellation_request_date,
         membership_expiration_date,
         membership_source,
         membership_status,
         replace(replace(replace(replace(replace(replace(membership_type,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t') membership_type,
         membership_current_price,
         replace(replace(replace(replace(replace(replace(membership_termination_reason,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t')membership_termination_reason,
         membership_product_id,
         replace(replace(replace(replace(replace(replace(membership_product_description,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t')membership_product_description,
         sum_expected_value_60_months,
         replace(replace(replace(replace(replace(replace(primary_activity_segment_name,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t')primary_activity_segment_name,
         replace(replace(replace(replace(replace(replace(term_risk_segment_name,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t')term_risk_segment_name,
         nps_score,
         nps_survey_date,
         promotion_opt_in,
         global_opt_in
    from wrk_pega_customer_attribute
   where dv_batch_id = @dv_batch_id
     and sequence_number >= @start
     and sequence_number < @end

  -- Get the related customer attribute interests
  -- replace any special characters in JSON with escape sequences
  if object_id('tempdb..#wrk_pega_customer_attribute_interest') is not null drop table #wrk_pega_customer_attribute_interest
  create table dbo.#wrk_pega_customer_attribute_interest with (distribution = hash (dim_mms_member_key),location = user_db) as
  select dim_mms_member_key,
         interest_id,
         replace(replace(replace(replace(replace(replace(interest_name,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t')interest_name,
         interest_confidence
    from wrk_pega_customer_attribute_interest
   where dv_batch_id = @dv_batch_id
     and dim_mms_member_key in (select dim_mms_member_key from #wrk_pega_customer_attribute)

  -- Create an array of interests grouping by member
  if object_id('tempdb..#wrk_pega_customer_attribute_interest_list') is not null drop table #wrk_pega_customer_attribute_interest_list
  create table dbo.#wrk_pega_customer_attribute_interest_list with (distribution = hash (dim_mms_member_key),location = user_db) as
  select dim_mms_member_key,
         '['
       + string_agg('{'
                  + '"interest_id":' + isnull(convert(varchar(4000),ca_member_interest.interest_id),'null') + ','
                  + '"interest_name":' + isnull('"' + convert(varchar(4000),ltrim(rtrim(ca_member_interest.interest_name)) + '"'),'null') + ','
                  + '"interest_confidence":' + isnull(convert(varchar(4000),ca_member_interest.interest_confidence),'null')
                  + '}'
                    , ',') within group (order by ca_member_interest.interest_id asc)
       + ']' interest_list
    from #wrk_pega_customer_attribute_interest ca_member_interest
   group by dim_mms_member_key

  -- Generate the json
  select case when ca_member.sequence_number != @start then ',' else '' end
       + '{'
       + '"membership_id":' + isnull(convert(varchar(4000),ca_member.membership_id),'null') + ','
       + '"member_id":' + isnull(convert(varchar(4000),ca_member.member_id),'null') + ','
       + '"entity_id":' + isnull(convert(varchar(4000),ca_member.entity_id),'null') + ','
       + '"first_name":' + isnull('"' + convert(varchar(4000),ca_member.first_name) + '"','null') + ','
       + '"last_name":' + isnull('"' + convert(varchar(4000),ca_member.last_name) + '"','null') + ','
       + '"date_of_birth":' + isnull('"' + convert(varchar(4000),ca_member.date_of_birth) + '"','null') + ','
       + '"member_active_flag":' + isnull('"' + convert(varchar(4000),ca_member.member_active_flag) + '"','null') + ','
       + '"member_gender":' + isnull('"' + convert(varchar(4000),ca_member.member_gender) + '"','null') + ','
       + '"assess_junior_member_dues_flag":' + isnull('"' + convert(varchar(4000),ca_member.assess_junior_member_dues_flag) + '"','null') + ','
       + '"member_type":' + isnull('"' + convert(varchar(4000),ca_member.member_type) + '"','null') + ','
       + '"member_join_date":' + isnull('"' + convert(varchar(4000),ca_member.member_join_date) + '"','null') + ','
       + '"party_id":' + isnull(convert(varchar(4000),ca_member.party_id),'null') + ','
       + '"member_mms_home_club_id":' + isnull(convert(varchar(4000),ca_member.member_mms_home_club_id),'null') + ','
       + '"membership_created_date_time":' + isnull('"' + convert(varchar(4000),ca_member.membership_created_date_time,126) + '"','null') + ','
       + '"membership_phone":' + isnull('"' + convert(varchar(4000),ca_member.membership_phone) + '"','null') + ','
       + '"membership_address_line_1":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_line_1) + '"','null') + ','
       + '"membership_address_line_2":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_line_2) + '"','null') + ','
       + '"membership_address_city":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_city) + '"','null') + ','
       + '"membership_address_country":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_country) + '"','null') + ','
       + '"membership_address_postal_code":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_postal_code) + '"','null') + ','
       + '"membership_address_state_abbreviation":' + isnull('"' + convert(varchar(4000),ca_member.membership_address_state_abbreviation) + '"','null') + ','
       + '"membership_cancellation_request_date":' + isnull('"' + convert(varchar(4000),ca_member.membership_cancellation_request_date) + '"','null') + ','
       + '"membership_expiration_date":' + isnull('"' + convert(varchar(4000),ca_member.membership_expiration_date) + '"','null') + ','
       + '"membership_source":' + isnull('"' + convert(varchar(4000),ca_member.membership_source) + '"','null') + ','
       + '"membership_status":' + isnull('"' + convert(varchar(4000),ca_member.membership_status) + '"','null') + ','
       + '"membership_type":' + isnull('"' + convert(varchar(4000),ca_member.membership_type) + '"','null') + ','
       + '"membership_current_price":' + isnull(convert(varchar(4000),ca_member.membership_current_price),'null') + ','
       + '"membership_termination_reason":' + isnull('"' + convert(varchar(4000),ca_member.membership_termination_reason) + '"','null') + ','
       + '"membership_product_id":' + isnull(convert(varchar(4000),ca_member.membership_product_id),'null') + ','
       + '"membership_product_description":' + isnull('"' + convert(varchar(4000),ltrim(rtrim(ca_member.membership_product_description))) + '"','null') + ','
       + '"sum_expected_value_60_months":' + isnull(convert(varchar(4000),ca_member.sum_expected_value_60_months),'null') + ','
       + '"primary_activity_segment_name":' + isnull('"' + convert(varchar(4000),ca_member.primary_activity_segment_name) + '"','null') + ','
       + '"term_risk_segment_name":' + isnull('"' + convert(varchar(4000),ca_member.term_risk_segment_name) + '"','null') + ','
       + '"nps_score":' + isnull('"' + convert(varchar(4000),ca_member.nps_score) + '"','null') + ','
       + '"nps_survey_date":' + isnull('"' + convert(varchar(4000),ca_member.nps_survey_date) + '"','null') + ','
       + '"promotion_opt_in":' + case ca_member.promotion_opt_in when 0 then 'false' when 1 then 'true' else 'null' end + ','
       + '"global_opt_in":' + case ca_member.global_opt_in when 0 then 'false' when 1 then 'true' else 'null' end + ','
       + '"interests":' + isnull(ca_member_interest_list.interest_list,'null')
       + '}' json_output
    from #wrk_pega_customer_attribute ca_member
    left join #wrk_pega_customer_attribute_interest_list ca_member_interest_list
      on ca_member.dim_mms_member_key = ca_member_interest_list.dim_mms_member_key
   order by ca_member.sequence_number

end
