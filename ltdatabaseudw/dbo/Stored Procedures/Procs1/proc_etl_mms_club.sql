CREATE PROC [dbo].[proc_etl_mms_club] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Club

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Club (
       bk_hash,
       ClubID,
       ValRegionID,
       StatementMessageID,
       ValClubTypeID,
       DomainNamePrefix,
       ClubName,
       ReceiptFooter,
       DisplayUIFlag,
       CheckInGroupLevel,
       ValStatementTypeID,
       ChargeToAccountFlag,
       ValPreSaleID,
       ClubActivationDate,
       ValTimeZoneID,
       InsertedDateTime,
       ValCWRegionID,
       EFTGroupID,
       GLTaxID,
       GLClubID,
       CRMDivisionCode,
       AssessJrMemberDuesFlag,
       SellJrMemberDuesFlag,
       UpdatedDateTime,
       ClubCode,
       SiteID,
       NewMemberCardFlag,
       ValMemberActivityRegionID,
       IGStoreID,
       ChildCenterWeeklyLimit,
       ValSalesAreaID,
       ValPTRCLAreaID,
       FormalClubName,
       KronosForecastMapPath,
       ClubDeActivationDate,
       GLCashEntryAccount,
       GLReceivablesEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntrySubAccount,
       GLCashEntryCompanyName,
       GLReceivablesEntryCompanyName,
       MarketingMapRegion,
       MarketingMapXmlStateName,
       MarketingClubLevel,
       ValCurrencyCodeID,
       AllowMultipleCurrencyFlag,
       WorkdayRegion,
       AllowJuniorCheckInFlag,
       LTFResourceID,
       HealthClubIdentifier,
       MaxJuniorAge,
       MaxSecondaryAge,
       ChargeNextMonthDate,
       MinFrontDeskCheckinAge,
       MaxChildCenterCheckinAge,
       StateCancellationDays,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClubID,
       ValRegionID,
       StatementMessageID,
       ValClubTypeID,
       DomainNamePrefix,
       ClubName,
       ReceiptFooter,
       DisplayUIFlag,
       CheckInGroupLevel,
       ValStatementTypeID,
       ChargeToAccountFlag,
       ValPreSaleID,
       ClubActivationDate,
       ValTimeZoneID,
       InsertedDateTime,
       ValCWRegionID,
       EFTGroupID,
       GLTaxID,
       GLClubID,
       CRMDivisionCode,
       AssessJrMemberDuesFlag,
       SellJrMemberDuesFlag,
       UpdatedDateTime,
       ClubCode,
       SiteID,
       NewMemberCardFlag,
       ValMemberActivityRegionID,
       IGStoreID,
       ChildCenterWeeklyLimit,
       ValSalesAreaID,
       ValPTRCLAreaID,
       FormalClubName,
       KronosForecastMapPath,
       ClubDeActivationDate,
       GLCashEntryAccount,
       GLReceivablesEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntrySubAccount,
       GLCashEntryCompanyName,
       GLReceivablesEntryCompanyName,
       MarketingMapRegion,
       MarketingMapXmlStateName,
       MarketingClubLevel,
       ValCurrencyCodeID,
       AllowMultipleCurrencyFlag,
       WorkdayRegion,
       AllowJuniorCheckInFlag,
       LTFResourceID,
       HealthClubIdentifier,
       MaxJuniorAge,
       MaxSecondaryAge,
       ChargeNextMonthDate,
       MinFrontDeskCheckinAge,
       MaxChildCenterCheckinAge,
       StateCancellationDays,
       isnull(cast(stage_mms_Club.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_Club
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_club @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_club (
       bk_hash,
       club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_Club.bk_hash,
       stage_hash_mms_Club.ClubID club_id,
       isnull(cast(stage_hash_mms_Club.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Club
  left join h_mms_club
    on stage_hash_mms_Club.bk_hash = h_mms_club.bk_hash
 where h_mms_club_id is null
   and stage_hash_mms_Club.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_club
if object_id('tempdb..#l_mms_club_inserts') is not null drop table #l_mms_club_inserts
create table #l_mms_club_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Club.bk_hash,
       stage_hash_mms_Club.ClubID club_id,
       stage_hash_mms_Club.ValRegionID val_region_id,
       stage_hash_mms_Club.StatementMessageID statement_message_id,
       stage_hash_mms_Club.ValClubTypeID val_club_type_id,
       stage_hash_mms_Club.ValStatementTypeID val_statement_type_id,
       stage_hash_mms_Club.ValPreSaleID val_pre_sale_id,
       stage_hash_mms_Club.ValTimeZoneID val_time_zone_id,
       stage_hash_mms_Club.ValCWRegionID val_cw_region_id,
       stage_hash_mms_Club.EFTGroupID eft_group_id,
       stage_hash_mms_Club.GLTaxID gl_tax_id,
       stage_hash_mms_Club.GLClubID gl_club_id,
       stage_hash_mms_Club.SiteID site_id,
       stage_hash_mms_Club.ValMemberActivityRegionID val_member_activity_region_id,
       stage_hash_mms_Club.IGStoreID ig_store_id,
       stage_hash_mms_Club.ValSalesAreaID val_sales_area_id,
       stage_hash_mms_Club.ValPTRCLAreaID val_pt_rcl_area_id,
       stage_hash_mms_Club.ValCurrencyCodeID val_currency_code_id,
       stage_hash_mms_Club.LTFResourceID ltf_resource_id,
       isnull(cast(stage_hash_mms_Club.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValRegionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.StatementMessageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValClubTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValStatementTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValPreSaleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValTimeZoneID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValCWRegionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.EFTGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.GLTaxID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.GLClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.SiteID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValMemberActivityRegionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.IGStoreID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValSalesAreaID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValPTRCLAreaID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ValCurrencyCodeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.LTFResourceID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Club
 where stage_hash_mms_Club.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_club records
set @insert_date_time = getdate()
insert into l_mms_club (
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
select #l_mms_club_inserts.bk_hash,
       #l_mms_club_inserts.club_id,
       #l_mms_club_inserts.val_region_id,
       #l_mms_club_inserts.statement_message_id,
       #l_mms_club_inserts.val_club_type_id,
       #l_mms_club_inserts.val_statement_type_id,
       #l_mms_club_inserts.val_pre_sale_id,
       #l_mms_club_inserts.val_time_zone_id,
       #l_mms_club_inserts.val_cw_region_id,
       #l_mms_club_inserts.eft_group_id,
       #l_mms_club_inserts.gl_tax_id,
       #l_mms_club_inserts.gl_club_id,
       #l_mms_club_inserts.site_id,
       #l_mms_club_inserts.val_member_activity_region_id,
       #l_mms_club_inserts.ig_store_id,
       #l_mms_club_inserts.val_sales_area_id,
       #l_mms_club_inserts.val_pt_rcl_area_id,
       #l_mms_club_inserts.val_currency_code_id,
       #l_mms_club_inserts.ltf_resource_id,
       case when l_mms_club.l_mms_club_id is null then isnull(#l_mms_club_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_club_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_club_inserts
  left join p_mms_club
    on #l_mms_club_inserts.bk_hash = p_mms_club.bk_hash
   and p_mms_club.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_club
    on p_mms_club.bk_hash = l_mms_club.bk_hash
   and p_mms_club.l_mms_club_id = l_mms_club.l_mms_club_id
 where l_mms_club.l_mms_club_id is null
    or (l_mms_club.l_mms_club_id is not null
        and l_mms_club.dv_hash <> #l_mms_club_inserts.source_hash)

--calculate hash and lookup to current s_mms_club
if object_id('tempdb..#s_mms_club_inserts') is not null drop table #s_mms_club_inserts
create table #s_mms_club_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Club.bk_hash,
       stage_hash_mms_Club.ClubID club_id,
       stage_hash_mms_Club.DomainNamePrefix domain_name_prefix,
       stage_hash_mms_Club.ClubName club_name,
       stage_hash_mms_Club.ReceiptFooter receipt_footer,
       stage_hash_mms_Club.DisplayUIFlag display_ui_flag,
       stage_hash_mms_Club.CheckInGroupLevel check_in_group_level,
       stage_hash_mms_Club.ChargeToAccountFlag charge_to_account_flag,
       stage_hash_mms_Club.ClubActivationDate club_activation_date,
       stage_hash_mms_Club.InsertedDateTime inserted_date_time,
       stage_hash_mms_Club.CRMDivisionCode crm_division_code,
       stage_hash_mms_Club.AssessJrMemberDuesFlag assess_junior_member_dues_flag,
       stage_hash_mms_Club.SellJrMemberDuesFlag sell_junior_member_dues_flag,
       stage_hash_mms_Club.ClubCode club_code,
       stage_hash_mms_Club.NewMemberCardFlag new_member_card_flag,
       stage_hash_mms_Club.ChildCenterWeeklyLimit child_center_weekly_limit,
       stage_hash_mms_Club.FormalClubName formal_club_name,
       stage_hash_mms_Club.KronosForecastMapPath kronos_forecast_map_path,
       stage_hash_mms_Club.ClubDeActivationDate club_deactivation_date,
       stage_hash_mms_Club.GLCashEntryAccount gl_cash_entry_account,
       stage_hash_mms_Club.GLReceivablesEntryAccount gl_receivables_entry_account,
       stage_hash_mms_Club.GLCashEntryCashSubAccount gl_cash_entry_cash_sub_account,
       stage_hash_mms_Club.GLCashEntryCreditCardSubAccount gl_cash_entry_credit_card_sub_account,
       stage_hash_mms_Club.GLReceivablesEntrySubAccount gl_receivables_entry_sub_account,
       stage_hash_mms_Club.GLCashEntryCompanyName gl_cash_entry_company_name,
       stage_hash_mms_Club.GLReceivablesEntryCompanyName gl_receivables_entry_company_name,
       stage_hash_mms_Club.MarketingMapRegion marketing_map_region,
       stage_hash_mms_Club.MarketingMapXmlStateName marketing_map_xml_state_name,
       stage_hash_mms_Club.MarketingClubLevel marketing_club_level,
       stage_hash_mms_Club.AllowMultipleCurrencyFlag allow_multiple_currency_flag,
       stage_hash_mms_Club.WorkdayRegion workday_region,
       stage_hash_mms_Club.AllowJuniorCheckInFlag allow_junior_check_in_flag,
       stage_hash_mms_Club.HealthClubIdentifier health_mms_club_identifier,
       stage_hash_mms_Club.MaxJuniorAge max_junior_age,
       stage_hash_mms_Club.MaxSecondaryAge max_secondary_age,
       stage_hash_mms_Club.ChargeNextMonthDate charge_next_month_date,
       stage_hash_mms_Club.MinFrontDeskCheckinAge min_front_desk_checkin_age,
       stage_hash_mms_Club.MaxChildCenterCheckinAge max_child_center_checkin_age,
       stage_hash_mms_Club.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_Club.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.DomainNamePrefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.ClubName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.ReceiptFooter,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.DisplayUIFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.CheckInGroupLevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ChargeToAccountFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Club.ClubActivationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Club.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.CRMDivisionCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.AssessJrMemberDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.SellJrMemberDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.ClubCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.NewMemberCardFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ChildCenterWeeklyLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.FormalClubName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.KronosForecastMapPath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Club.ClubDeActivationDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLCashEntryAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLReceivablesEntryAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLCashEntryCashSubAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLCashEntryCreditCardSubAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLReceivablesEntrySubAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLCashEntryCompanyName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.GLReceivablesEntryCompanyName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.MarketingMapRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.MarketingMapXmlStateName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.MarketingClubLevel,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.AllowMultipleCurrencyFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.WorkdayRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.AllowJuniorCheckInFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Club.HealthClubIdentifier,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.MaxJuniorAge as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.MaxSecondaryAge as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.ChargeNextMonthDate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.MinFrontDeskCheckinAge as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Club.MaxChildCenterCheckinAge as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Club.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Club
 where stage_hash_mms_Club.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_club records
set @insert_date_time = getdate()
insert into s_mms_club (
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
select #s_mms_club_inserts.bk_hash,
       #s_mms_club_inserts.club_id,
       #s_mms_club_inserts.domain_name_prefix,
       #s_mms_club_inserts.club_name,
       #s_mms_club_inserts.receipt_footer,
       #s_mms_club_inserts.display_ui_flag,
       #s_mms_club_inserts.check_in_group_level,
       #s_mms_club_inserts.charge_to_account_flag,
       #s_mms_club_inserts.club_activation_date,
       #s_mms_club_inserts.inserted_date_time,
       #s_mms_club_inserts.crm_division_code,
       #s_mms_club_inserts.assess_junior_member_dues_flag,
       #s_mms_club_inserts.sell_junior_member_dues_flag,
       #s_mms_club_inserts.club_code,
       #s_mms_club_inserts.new_member_card_flag,
       #s_mms_club_inserts.child_center_weekly_limit,
       #s_mms_club_inserts.formal_club_name,
       #s_mms_club_inserts.kronos_forecast_map_path,
       #s_mms_club_inserts.club_deactivation_date,
       #s_mms_club_inserts.gl_cash_entry_account,
       #s_mms_club_inserts.gl_receivables_entry_account,
       #s_mms_club_inserts.gl_cash_entry_cash_sub_account,
       #s_mms_club_inserts.gl_cash_entry_credit_card_sub_account,
       #s_mms_club_inserts.gl_receivables_entry_sub_account,
       #s_mms_club_inserts.gl_cash_entry_company_name,
       #s_mms_club_inserts.gl_receivables_entry_company_name,
       #s_mms_club_inserts.marketing_map_region,
       #s_mms_club_inserts.marketing_map_xml_state_name,
       #s_mms_club_inserts.marketing_club_level,
       #s_mms_club_inserts.allow_multiple_currency_flag,
       #s_mms_club_inserts.workday_region,
       #s_mms_club_inserts.allow_junior_check_in_flag,
       #s_mms_club_inserts.health_mms_club_identifier,
       #s_mms_club_inserts.max_junior_age,
       #s_mms_club_inserts.max_secondary_age,
       #s_mms_club_inserts.charge_next_month_date,
       #s_mms_club_inserts.min_front_desk_checkin_age,
       #s_mms_club_inserts.max_child_center_checkin_age,
       #s_mms_club_inserts.updated_date_time,
       case when s_mms_club.s_mms_club_id is null then isnull(#s_mms_club_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_club_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_club_inserts
  left join p_mms_club
    on #s_mms_club_inserts.bk_hash = p_mms_club.bk_hash
   and p_mms_club.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_club
    on p_mms_club.bk_hash = s_mms_club.bk_hash
   and p_mms_club.s_mms_club_id = s_mms_club.s_mms_club_id
 where s_mms_club.s_mms_club_id is null
    or (s_mms_club.s_mms_club_id is not null
        and s_mms_club.dv_hash <> #s_mms_club_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_club @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_club @current_dv_batch_id
exec dbo.proc_d_mms_club_history @current_dv_batch_id

end
