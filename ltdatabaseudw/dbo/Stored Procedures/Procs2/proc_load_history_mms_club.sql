CREATE PROC [dbo].[proc_load_history_mms_club] AS
begin

set nocount on
set xact_abort on

 /*prepare the dv tables*/
truncate table dbo.h_mms_club
truncate table dbo.l_mms_club
truncate table dbo.s_mms_club

exec proc_util_create_base_records @table_name = 'h_mms_club'
exec proc_util_create_base_records @table_name = 'l_mms_club'
exec proc_util_create_base_records @table_name = 's_mms_club'

/* Create historical pit proc*/
exec [proc_util_generate_procedure_pit_historical] 'p_mms_club'

/*Select the records from [dbo].[MMSClub] to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_Club_history') is not null drop table #stage_mms_Club_history
create table dbo.#stage_mms_Club_history with (location=user_db, distribution = hash(ClubID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
								
        convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(DomainNamePrefix,'z#@$k%&P')
										+'P%#&z$@k'+isnull(ClubName,'z#@$k%&P')
										+'P%#&z$@k'+isnull(ReceiptFooter,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(DisplayUIFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(CheckInGroupLevel as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ChargeToAccountFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,ClubActivationDate,120),'z#@$k%&P')
										+'P%#&z$@k'+isnull(convert(varchar,MMSInsertedDateTime,120),'z#@$k%&P')
										+'P%#&z$@k'+isnull(CRMDivisionCode,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(SellJrMemberDuesFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(ClubCode,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(NewMemberCardFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ChildCenterWeeklyLimit as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(FormalClubName,'z#@$k%&P')
										+'P%#&z$@k'+isnull(KronosForecastMapPath,'z#@$k%&P')
										+'P%#&z$@k'+isnull(convert(varchar,ClubDeActivationDate,120),'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLCashEntryAccount,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLReceivablesEntryAccount,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLCashEntryCashSubAccount,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLCashEntryCreditCardSubAccount,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLReceivablesEntrySubAccount,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLCashEntryCompanyName,'z#@$k%&P')
										+'P%#&z$@k'+isnull(GLReceivablesEntryCompanyName,'z#@$k%&P')
										+'P%#&z$@k'+isnull(MarketingMapRegion,'z#@$k%&P')
										+'P%#&z$@k'+isnull(MarketingMapXmlStateName,'z#@$k%&P')
										+'P%#&z$@k'+isnull(MarketingClubLevel,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(AllowMultipleCurrencyFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(WorkdayRegion,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(AllowJuniorCheckInFlag as varchar(42)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(HealthClubIdentifier,'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(MaxJuniorAge as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(MaxSecondaryAge as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ChargeNextMonthDate as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(MinFrontDeskCheckinAge as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(MaxChildCenterCheckinAge as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(convert(varchar,MMSUpdatedDateTime,120),'z#@$k%&P'))),2) AS s_mms_club_hash ,
	   convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValRegionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(StatementMessageID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValClubTypeID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValStatementTypeID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValPreSaleID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValTimeZoneID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValCWRegionID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(EFTGroupID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(GLTaxID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(GLClubID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(SiteID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValMemberActivityRegionID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(IGStoreID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValSalesAreaID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValPTRCLAreaID as varchar(500)),'z#@$k%&P')
										+'P%#&z$@k'+isnull(cast(ValCurrencyCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(LTFResourceID as varchar(500)),'z#@$k%&P'))),2) AS  l_mms_club_hash,

        row_number() over(partition by ClubID order by x.update_insert_date) rank2,
		--row_number() over(partition by ClubID,l_mms_club_hash order by x.update_insert_date desc) rank3,
		*
  from (select row_number() over(partition by ClubID,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
               end 
			   order by MMSClubKey desc) rank1,				 
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   else MMSUpdatedDateTime
               end update_insert_date,
               *
          from stage_mms_Club_history) x
where rank1 = 1
                              
/* Create the h records.*/
/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_club(
	   bk_hash,
       club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
              #stage_mms_Club_history.ClubID ,
               isnull(cast(#stage_mms_Club_history.MMSInsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_Club_history 
         where rank2 = 1) x
         		
/* Create the l records.*/
/* Calculate dv_load_date_time*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.l_mms_club (
		bk_hash,
        club_id,
		val_region_id,
		statement_message_id,
		val_club_type_id,
		val_statement_type_id,
		val_pre_sale_id,
		val_time_zone_id,
		val_cw_region_id,
		eft_group_id,
		gl_tax_id,
		gl_club_id,
		site_id,
		val_member_activity_region_id,
		ig_store_id,
		val_sales_area_id,
		val_pt_rcl_area_id,
		val_currency_code_id,
		ltf_resource_id,
        dv_load_date_time,
        dv_batch_id,
        dv_r_load_source_id,
        dv_hash,
        dv_inserted_date_time,
        dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_Club_history.bk_hash,
       #stage_mms_Club_history.ClubID club_id,
       #stage_mms_Club_history.ValRegionID,
       #stage_mms_Club_history.StatementMessageID,
       #stage_mms_Club_history.ValClubTypeID,
	   #stage_mms_Club_history.ValStatementTypeID,
	   #stage_mms_Club_history.ValPreSaleID,
	   #stage_mms_Club_history.ValTimeZoneID,
	   #stage_mms_Club_history.ValCWRegionID,
	   #stage_mms_Club_history.EFTGroupID,
	   #stage_mms_Club_history.GLTaxID,
	   #stage_mms_Club_history.GLClubID,
	   #stage_mms_Club_history.SiteID,
	   #stage_mms_Club_history.ValMemberActivityRegionID,
	   #stage_mms_Club_history.IGStoreID,
	   #stage_mms_Club_history.ValSalesAreaID,
	   #stage_mms_Club_history.ValPTRCLAreaID,
	   #stage_mms_Club_history.ValCurrencyCodeID,
	   #stage_mms_Club_history.LTFResourceID,
	   
              case when #stage_mms_Club_history.rank2 = 1 then
                         case when #stage_mms_Club_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Club_history.MMSInsertedDateTime
                          end
					         else isnull(#stage_mms_Club_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107))
               end dv_load_date_time,
               case when #stage_mms_Club_history.rank2 = 1 then
                         case when #stage_mms_Club_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Club_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    else replace(replace(replace(convert(varchar, isnull(#stage_mms_Club_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')	  
			  end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Club_history.l_mms_club_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_Club_history
           left join dbo.#stage_mms_Club_history prior
             on #stage_mms_Club_history.ClubID = prior.ClubID
           and #stage_mms_Club_history.rank2 = prior.rank2 + 1
		   --where  #stage_mms_Club_history.rank3=1 )x
          where #stage_mms_Club_history.l_mms_club_hash != isnull(prior.l_mms_club_hash, ''))x

/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_club (
		bk_hash,
		club_id,
		domain_name_prefix,
		club_name,
		receipt_footer,
		display_ui_flag,
		check_in_group_level,
		charge_to_account_flag,
		club_activation_date,
		inserted_date_time,
		crm_division_code,
		assess_junior_member_dues_flag,
		sell_junior_member_dues_flag,
		club_code,
		new_member_card_flag,
		child_center_weekly_limit,
		formal_club_name,
		kronos_forecast_map_path,
		club_deactivation_date,
		gl_cash_entry_account,
		gl_receivables_entry_account,
		gl_cash_entry_cash_sub_account,
		gl_cash_entry_credit_card_sub_account,
		gl_receivables_entry_sub_account,
		gl_cash_entry_company_name,
		gl_receivables_entry_company_name,
		marketing_map_region,
		marketing_map_xml_state_name,
		marketing_club_level,
		allow_multiple_currency_flag,
		workday_region,
		allow_junior_check_in_flag,
		health_mms_club_identifier,
		max_junior_age,
		max_secondary_age,
		charge_next_month_date,
		min_front_desk_checkin_age,
		max_child_center_checkin_age,
		updated_date_time,
        dv_load_date_time,
        dv_batch_id,
        dv_r_load_source_id,
        dv_hash,
        dv_inserted_date_time,
        dv_insert_user)
select 
       x.*
  from (select #stage_mms_Club_history.bk_hash,
		#stage_mms_Club_history.ClubID,
		#stage_mms_Club_history.DomainNamePrefix,
		#stage_mms_Club_history.ClubName,
		#stage_mms_Club_history.ReceiptFooter,
		#stage_mms_Club_history.DisplayUIFlag,
		#stage_mms_Club_history.CheckInGroupLevel,
		#stage_mms_Club_history.ChargeToAccountFlag,
		#stage_mms_Club_history.ClubActivationDate,
		#stage_mms_Club_history.MMSInsertedDateTime,
		#stage_mms_Club_history.CRMDivisionCode,
		#stage_mms_Club_history.AssessJrMemberDuesFlag,
		#stage_mms_Club_history.SellJrMemberDuesFlag,
		#stage_mms_Club_history.ClubCode,
		#stage_mms_Club_history.NewMemberCardFlag,
		#stage_mms_Club_history.ChildCenterWeeklyLimit,
		#stage_mms_Club_history.FormalClubName,
		#stage_mms_Club_history.KronosForecastMapPath,
		#stage_mms_Club_history.ClubDeActivationDate,
		#stage_mms_Club_history.GLCashEntryAccount,
		#stage_mms_Club_history.GLReceivablesEntryAccount,
		#stage_mms_Club_history.GLCashEntryCashSubAccount,
		#stage_mms_Club_history.GLCashEntryCreditCardSubAccount,
		#stage_mms_Club_history.GLReceivablesEntrySubAccount,
		#stage_mms_Club_history.GLCashEntryCompanyName,
		#stage_mms_Club_history.GLReceivablesEntryCompanyName,
		#stage_mms_Club_history.MarketingMapRegion,
		#stage_mms_Club_history.MarketingMapXmlStateName,
		#stage_mms_Club_history.MarketingClubLevel,
		#stage_mms_Club_history.AllowMultipleCurrencyFlag,
		#stage_mms_Club_history.WorkdayRegion,
		#stage_mms_Club_history.AllowJuniorCheckInFlag,
		#stage_mms_Club_history.HealthClubIdentifier,
		#stage_mms_Club_history.MaxJuniorAge,
		#stage_mms_Club_history.MaxSecondaryAge,
		#stage_mms_Club_history.ChargeNextMonthDate,
		#stage_mms_Club_history.MinFrontDeskCheckinAge,
		#stage_mms_Club_history.MaxChildCenterCheckinAge,
		#stage_mms_Club_history.MMSUpdatedDateTime,
               case when #stage_mms_Club_history.rank2 = 1 then
                         case when #stage_mms_Club_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_Club_history.MMSInsertedDateTime
                          end
                   else isnull(#stage_mms_Club_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107))  
			   end dv_load_date_time,
               case when #stage_mms_Club_history.rank2 = 1 then
                         case when #stage_mms_Club_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_Club_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                   else replace(replace(replace(convert(varchar, isnull(#stage_mms_Club_history.MMSUpdatedDateTime,convert(datetime,'jan 1, 1980',107)),120), '-', ''),' ', ''), ':', '')	  
			 end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_Club_history.s_mms_club_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_Club_history
          left join dbo.#stage_mms_Club_history prior
            on #stage_mms_Club_history.ClubID = prior.ClubID
           and #stage_mms_Club_history.rank2 = prior.rank2 + 1
         where #stage_mms_Club_history.s_mms_club_hash != isnull(prior.s_mms_club_hash, ''))x


/* Populate the pit table*/
truncate table dbo.p_mms_club						
exec dbo.proc_p_mms_club @current_dv_batch_id = -1 

/*------------------Include in proc(D table data Load)*/

delete from dv_d_etl_map where target_object = 'v_dim_mms_club_history'

exec [dbo].[proc_util_create_simple_view_records] 'dim_club', 'DW_2019_10_16', 'marketing', 'v_dim_mms_club_history'

delete from dv_d_etl_map where target_object = 'v_dim_mms_club_history' and target_column in(
'dim_club_key',
'club_id',
'effective_date_time',
'expiration_date_time',
'allow_junior_check_in_flag',
'assess_junior_member_dues_flag',
'check_in_group_level',
'child_center_weekly_limit',
'club_close_dim_date_key',
'club_code',
'club_name',
'club_open_dim_date_key',
'club_status',
'club_type',
'info_genesis_store_id',
'currency_code_dim_description_key',
'domain_name_prefix',
'formal_club_name',
'gl_club_id',
'marketing_map_region',
'max_junior_age',
'marketing_club_level',
'member_activities_region_dim_description_key',
'region_dim_description_key',
'pt_rcl_area_dim_description_key',
'sales_area_dim_description_key',
'sell_junior_member_dues_flag',
'workday_region',
'val_member_activity_region_id',
'val_pt_rcl_area_id',
'val_region_id',
'val_sales_area_id',
'val_time_zone_id',
'dst_offset',
'st_offset')

insert into dv_d_etl_map (target_object, target_column, data_type, source_sql, release, dv_inserted_date_time, dv_insert_user)
select 'v_dim_mms_club_history' target_object,'dim_club_key' target_column,'char(32)' data_type,'d_mms_club_history.dim_club_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_id' target_column,'int' data_type,'d_mms_club_history.club_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'effective_date_time' target_column,'datetime' data_type,'isnull(d_mms_club_history.effective_date_time,'+'''1/1/1900'''+')' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'expiration_date_time' target_column,'datetime' data_type,'isnull(d_mms_club_history.expiration_date_time,'+'''12/31/2200'''+')' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'allow_junior_check_in_flag' target_column,'char(1)' data_type,'d_mms_club_history.allow_junior_check_in_flag' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'assess_junior_member_dues_flag' target_column,'char(1)' data_type,'d_mms_club_history.assess_junior_member_dues_flag' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'check_in_group_level' target_column,'int' data_type,'d_mms_club_history.check_in_group_level' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'child_center_weekly_limit' target_column,'int' data_type,'d_mms_club_history.child_center_weekly_limit' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_close_dim_date_key' target_column,'int' data_type,'d_mms_club_history.club_close_dim_date_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_code' target_column,'varchar(18)' data_type,'d_mms_club_history.club_code' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_name' target_column,'varchar(50)' data_type,'d_mms_club_history.club_name' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_open_dim_date_key' target_column,'int' data_type,'d_mms_club_history.club_open_dim_date_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_status' target_column,'varchar(25)' data_type,'d_mms_club_history.club_status' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'club_type' target_column,'varchar(21)' data_type,'d_mms_club_history.club_type' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'info_genesis_store_id' target_column,'int' data_type,'d_mms_club_history.info_genesis_store_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'currency_code_dim_description_key' target_column,'varchar(538)' data_type,'d_mms_club_history.currency_code_dim_description_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'domain_name_prefix' target_column,'varchar(10)' data_type,'d_mms_club_history.domain_name_prefix' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'formal_club_name' target_column,'varchar(50)' data_type,'d_mms_club_history.formal_club_name' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'gl_club_id' target_column,'int' data_type,'d_mms_club_history.gl_club_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'marketing_map_region' target_column,'varchar(50)' data_type,'d_mms_club_history.marketing_map_region' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'max_junior_age' target_column,'int' data_type,'d_mms_club_history.max_junior_age' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'marketing_club_level' target_column,'varchar(50)' data_type,'d_mms_club_history.marketing_club_level' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'member_activities_region_dim_description_key' target_column,'varchar(532)' data_type,'d_mms_club_history.member_activities_region_dim_description_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'region_dim_description_key' target_column,'varchar(532)' data_type,'d_mms_club_history.region_dim_description_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'pt_rcl_area_dim_description_key' target_column,'varchar(532)' data_type,'d_mms_club_history.pt_rcl_area_dim_description_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'sales_area_dim_description_key' target_column,'varchar(532)' data_type,'d_mms_club_history.sales_area_dim_description_key' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'sell_junior_member_dues_flag' target_column,'char(1)' data_type,'d_mms_club_history.sell_junior_member_dues_flag' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'workday_region' target_column,'varchar(4)' data_type,'d_mms_club_history.workday_region' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'val_member_activity_region_id' target_column,'int' data_type,'d_mms_club_history.val_member_activity_region_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'val_pt_rcl_area_id' target_column,'int' data_type,'d_mms_club_history.val_pt_rcl_area_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'val_region_id' target_column,'int' data_type,'d_mms_club_history.val_region_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'val_sales_area_id' target_column,'int' data_type,'d_mms_club_history.val_sales_area_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'val_time_zone_id' target_column,'int' data_type,'d_mms_club_history.val_time_zone_id' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'dst_offset' target_column,'int' data_type,'d_mms_club_history.dst_offset' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'st_offset' target_column,'int' data_type,'d_mms_club_history.st_offset' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user union
select 'v_dim_mms_club_history' target_object,'v_clause' target_column,'' data_type,'from dim_club  join d_mms_club_history  on dim_club.dim_club_key = d_mms_club_history.dim_club_key ' source_sql,'DW_2019_11_27' release, getdate() dv_inserted_date_time, suser_sname() dv_insert_user



if object_id('d_mms_club_history') is not null 
drop table d_mms_club_history

exec dbo.proc_util_generate_structures_d 'mms_club', 1

exec proc_d_mms_club_history '-1'


end

