CREATE PROC [dbo].[proc_etl_udwcloudsync_club_master_roster] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_udwcloudsync_ClubMasterRoster

set @insert_date_time = getdate()
insert into dbo.stage_hash_udwcloudsync_ClubMasterRoster (
       bk_hash,
       TwentyFourHourGuard,
       ThreeStory,
       AccessType,
       ActivityCenter,
       AllowJuniorCheckInFlag,
       AppCreatedBy,
       AppModifiedBy,
       ApprovalStatus,
       ApproverComments,
       AquaticsDH,
       Area,
       AreaDirector,
       AsstGeneralManager,
       AthleticLTA,
       Attachments,
       BasketballCourts,
       BasketballTrainer,
       BuldingEngineer,
       BusinessAdministrator,
       Cafe,
       CenterType,
       CheckInGroupLevel,
       ChildCenter,
       ChildCenterKidsActivitiesDH,
       ChildCenterWeeklyLimit,
       CIGEndDate,
       CIGStartDate,
       CIGStatus,
       ClassesOffered,
       ClientID,
       Clinic,
       ClubAddress,
       ClubID,
       ClubDeactivationDate,
       CognosName,
       ContentType,
       ContentTypeID,
       CopySource,
       Created,
       CreatedDate,
       CreatedBy,
       CurrentOperationsStatus,
       CycleStudios,
       DisplayUIFlag,
       Edit,
       EditMenuTableEnd,
       EditMenuTableStart,
       EffectivePermissionsMask,
       EGroupName,
       EmailAlert,
       EncodedAbsoluteURL,
       EngRCL,
       FacilityOperationsDH,
       FacilityTechnician,
       FamilySwim,
       [FileName],
       FileType,
       FolderChildCount,
       FormalClubName,
       GeneralManager,
       GLCashEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCompanyName,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntryAccount,
       GLReceivablesEntryCompanyName,
       GLReceivablesEntrySubAccount,
       GrandPrixTourLocation,
       GroupFitnessDH,
       GroupFitnessStudios,
       GrowthType,
       [GUID],
       HasCompanyDestinations,
       HealthClubIdentifier,
       HTMLFileType,
       HyperionPlanning,
       [ID],
       IGStoreID,
       IndoorPool,
       IndoorPoolCloseDate,
       IndoorPoolOpenDate,
       IndoorTennisCourts,
       InstanceID,
       IsCurrentVersion,
       ITCode,
       ItemChildCount,
       ItemType,
       KidsAcademy,
       LapPools,
       latitide,
       LeasesOwned,
       [Level],
       LifeCafeDH,
       LifeShop,
       LifeSpaDH,
       Longitude,
       MapCenterLatitude,
       MapCenterLongitude,
       MapZoomLevel,
       MarketingClubName,
       MarketingClubLevel,
       MarketingMapRegion,
       MarketingMapXMLStateName,
       MCAStudios,
       Medi,
       MemberEngagementManager,
       MemberServicesDH,
       MembershipLevel,
       MembershipLevelNeededForTennis,
       MMSClubID,
       MMSClubName,
       Modified,
       LastModified,
       ModifiedBy,
       Name_FileLeafRef,
       Name_LinkFilenameNoMenu,
       Name_LinkFileName,
       Name_LinkFileName2,
       NumberofSlides,
       NutritionCoach,
       OldKronosID,
       Open24Hours,
       [Open],
       OpenedDate,
       OpsRCLSrDH,
       [Order],
       OriginatorID,
       OutDoorPool,
       OutDoorPoolCloseDate,
       OutDoorPoolOpenDate,
       OutDoorTennisCourts,
       Owshiddenversion,
       ParentsNightOut,
       [Path],
       PersonalTrainingDH,
       PersonalTrainingRCM,
       PilatesStudio,
       PreSaleAddress,
       PriorYearOperationsStatus,
       ProactiveCare,
       ProgId,
       PropertyBag,
       PTRCMRegion,
       RacquetballCourts,
       RacquetballLeagueEndDate,
       RacquetballLeagueStartDate,
       Region,
       RegionalSalesManager,
       RegionalVicePresident,
       Restricted,
       RockWall,
       SalesCode,
       SchoolBreakCamps,
       ScopeId,
       SelectTitle,
       SeniorGeneralManager,
       ServerRelativeURL,
       SortType,
       Spa,
       SquareFootage,
       SquashCourts,
       SquashKids,
       State,
       StateAbbreviation,
       SubnetAddress,
       SummerBreakCamps,
       TennisCourts,
       TennisDH,
       Title,
       Title_LinkTitleNoMenu,
       Title_LinkTitle,
       Title_LinkTitle2,
       TurfField,
       [Type],
       UIVersion,
       UniqueFeatures,
       UniqueID,
       URLPath,
       ValActivityAreaIDLookup,
       ValActivityAreaIDLookupDescription,
       ValCheckInGroupIDLookup,
       ValCheckInGroupIDLookupDescription,
       ValClubTypeIDLookUp,
       ValClubTypeIDLookUpDescription,
       ValCountryIDLookup,
       ValCountryIDLookupTitle,
       ValCurrencyCodeIDLookup,
       ValCurrencyCodeIDLookupCurrencyCode,
       ValCWRegionIDLookup,
       ValCWRegionIDLookupDescription,
       ValEnrollmentTypeIDLookup,
       ValEnrollmentTypeIDLookupDescription,
       ValMemberActivityRegionIDLookup,
       ValMemberActivityRegionIDLookupTitle,
       ValPreSaleIDlookup,
       ValPreSaleIDlookupDescription,
       ValPTRCLAreaIDLookup,
       ValPTRCLAreaIDLookupTitle,
       ValRegionIDLookup,
       ValRegionIDLookupDescription,
       ValSalesAreaIDlookup,
       ValSalesAreaIDlookupDescription,
       ValStateIDLookup,
       ValStateIDLookupDescription,
       ValTimeZoneIDLookup,
       ValTimeZoneIDLookupDescription,
       [Version],
       WaterSlides,
       Whirpools,
       WorkDayRegionID,
       WorkflowInstanceID,
       WorkFlowVersion,
       YogaStudios,
       ZeroDepthEntry,
       EditMenuTableStart2,
       AquaticsRegionalManager,
       CafeRegionalManager,
       SpaRegionalManager,
       RacquetRegionalManager,
       PersonalTrainingRegionalManager,
       KidsRegionalManager,
       OperationsRegionalManager,
       SalesRegionalManager,
       GroupFitnessRegionalManager,
       MemberServicesRegionalManager,
       NutritionRegionalManager,
       SpaBiz_StoreNum,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MMSClubID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       TwentyFourHourGuard,
       ThreeStory,
       AccessType,
       ActivityCenter,
       AllowJuniorCheckInFlag,
       AppCreatedBy,
       AppModifiedBy,
       ApprovalStatus,
       ApproverComments,
       AquaticsDH,
       Area,
       AreaDirector,
       AsstGeneralManager,
       AthleticLTA,
       Attachments,
       BasketballCourts,
       BasketballTrainer,
       BuldingEngineer,
       BusinessAdministrator,
       Cafe,
       CenterType,
       CheckInGroupLevel,
       ChildCenter,
       ChildCenterKidsActivitiesDH,
       ChildCenterWeeklyLimit,
       CIGEndDate,
       CIGStartDate,
       CIGStatus,
       ClassesOffered,
       ClientID,
       Clinic,
       ClubAddress,
       ClubID,
       ClubDeactivationDate,
       CognosName,
       ContentType,
       ContentTypeID,
       CopySource,
       Created,
       CreatedDate,
       CreatedBy,
       CurrentOperationsStatus,
       CycleStudios,
       DisplayUIFlag,
       Edit,
       EditMenuTableEnd,
       EditMenuTableStart,
       EffectivePermissionsMask,
       EGroupName,
       EmailAlert,
       EncodedAbsoluteURL,
       EngRCL,
       FacilityOperationsDH,
       FacilityTechnician,
       FamilySwim,
       [FileName],
       FileType,
       FolderChildCount,
       FormalClubName,
       GeneralManager,
       GLCashEntryAccount,
       GLCashEntryCashSubAccount,
       GLCashEntryCompanyName,
       GLCashEntryCreditCardSubAccount,
       GLReceivablesEntryAccount,
       GLReceivablesEntryCompanyName,
       GLReceivablesEntrySubAccount,
       GrandPrixTourLocation,
       GroupFitnessDH,
       GroupFitnessStudios,
       GrowthType,
       [GUID],
       HasCompanyDestinations,
       HealthClubIdentifier,
       HTMLFileType,
       HyperionPlanning,
       [ID],
       IGStoreID,
       IndoorPool,
       IndoorPoolCloseDate,
       IndoorPoolOpenDate,
       IndoorTennisCourts,
       InstanceID,
       IsCurrentVersion,
       ITCode,
       ItemChildCount,
       ItemType,
       KidsAcademy,
       LapPools,
       latitide,
       LeasesOwned,
       [Level],
       LifeCafeDH,
       LifeShop,
       LifeSpaDH,
       Longitude,
       MapCenterLatitude,
       MapCenterLongitude,
       MapZoomLevel,
       MarketingClubName,
       MarketingClubLevel,
       MarketingMapRegion,
       MarketingMapXMLStateName,
       MCAStudios,
       Medi,
       MemberEngagementManager,
       MemberServicesDH,
       MembershipLevel,
       MembershipLevelNeededForTennis,
       MMSClubID,
       MMSClubName,
       Modified,
       LastModified,
       ModifiedBy,
       Name_FileLeafRef,
       Name_LinkFilenameNoMenu,
       Name_LinkFileName,
       Name_LinkFileName2,
       NumberofSlides,
       NutritionCoach,
       OldKronosID,
       Open24Hours,
       [Open],
       OpenedDate,
       OpsRCLSrDH,
       [Order],
       OriginatorID,
       OutDoorPool,
       OutDoorPoolCloseDate,
       OutDoorPoolOpenDate,
       OutDoorTennisCourts,
       Owshiddenversion,
       ParentsNightOut,
       [Path],
       PersonalTrainingDH,
       PersonalTrainingRCM,
       PilatesStudio,
       PreSaleAddress,
       PriorYearOperationsStatus,
       ProactiveCare,
       ProgId,
       PropertyBag,
       PTRCMRegion,
       RacquetballCourts,
       RacquetballLeagueEndDate,
       RacquetballLeagueStartDate,
       Region,
       RegionalSalesManager,
       RegionalVicePresident,
       Restricted,
       RockWall,
       SalesCode,
       SchoolBreakCamps,
       ScopeId,
       SelectTitle,
       SeniorGeneralManager,
       ServerRelativeURL,
       SortType,
       Spa,
       SquareFootage,
       SquashCourts,
       SquashKids,
       State,
       StateAbbreviation,
       SubnetAddress,
       SummerBreakCamps,
       TennisCourts,
       TennisDH,
       Title,
       Title_LinkTitleNoMenu,
       Title_LinkTitle,
       Title_LinkTitle2,
       TurfField,
       [Type],
       UIVersion,
       UniqueFeatures,
       UniqueID,
       URLPath,
       ValActivityAreaIDLookup,
       ValActivityAreaIDLookupDescription,
       ValCheckInGroupIDLookup,
       ValCheckInGroupIDLookupDescription,
       ValClubTypeIDLookUp,
       ValClubTypeIDLookUpDescription,
       ValCountryIDLookup,
       ValCountryIDLookupTitle,
       ValCurrencyCodeIDLookup,
       ValCurrencyCodeIDLookupCurrencyCode,
       ValCWRegionIDLookup,
       ValCWRegionIDLookupDescription,
       ValEnrollmentTypeIDLookup,
       ValEnrollmentTypeIDLookupDescription,
       ValMemberActivityRegionIDLookup,
       ValMemberActivityRegionIDLookupTitle,
       ValPreSaleIDlookup,
       ValPreSaleIDlookupDescription,
       ValPTRCLAreaIDLookup,
       ValPTRCLAreaIDLookupTitle,
       ValRegionIDLookup,
       ValRegionIDLookupDescription,
       ValSalesAreaIDlookup,
       ValSalesAreaIDlookupDescription,
       ValStateIDLookup,
       ValStateIDLookupDescription,
       ValTimeZoneIDLookup,
       ValTimeZoneIDLookupDescription,
       [Version],
       WaterSlides,
       Whirpools,
       WorkDayRegionID,
       WorkflowInstanceID,
       WorkFlowVersion,
       YogaStudios,
       ZeroDepthEntry,
       EditMenuTableStart2,
       AquaticsRegionalManager,
       CafeRegionalManager,
       SpaRegionalManager,
       RacquetRegionalManager,
       PersonalTrainingRegionalManager,
       KidsRegionalManager,
       OperationsRegionalManager,
       SalesRegionalManager,
       GroupFitnessRegionalManager,
       MemberServicesRegionalManager,
       NutritionRegionalManager,
       SpaBiz_StoreNum,
       isnull(cast(stage_udwcloudsync_ClubMasterRoster.created as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_udwcloudsync_ClubMasterRoster
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_udwcloudsync_club_master_roster @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_udwcloudsync_club_master_roster (
       bk_hash,
       mms_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_udwcloudsync_ClubMasterRoster.bk_hash,
       stage_hash_udwcloudsync_ClubMasterRoster.MMSClubID mms_club_id,
       isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.created as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       7,
       @insert_date_time,
       @user
  from stage_hash_udwcloudsync_ClubMasterRoster
  left join h_udwcloudsync_club_master_roster
    on stage_hash_udwcloudsync_ClubMasterRoster.bk_hash = h_udwcloudsync_club_master_roster.bk_hash
 where h_udwcloudsync_club_master_roster_id is null
   and stage_hash_udwcloudsync_ClubMasterRoster.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_udwcloudsync_club_master_roster
if object_id('tempdb..#l_udwcloudsync_club_master_roster_inserts') is not null drop table #l_udwcloudsync_club_master_roster_inserts
create table #l_udwcloudsync_club_master_roster_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_ClubMasterRoster.bk_hash,
       stage_hash_udwcloudsync_ClubMasterRoster.ClientID client_id,
       stage_hash_udwcloudsync_ClubMasterRoster.ITCode it_code,
       stage_hash_udwcloudsync_ClubMasterRoster.MMSClubID mms_club_id,
       stage_hash_udwcloudsync_ClubMasterRoster.OldKronosID old_kronos_id,
       stage_hash_udwcloudsync_ClubMasterRoster.SalesCode sales_code,
       stage_hash_udwcloudsync_ClubMasterRoster.ValActivityAreaIDLookup val_activity_area_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCheckInGroupIDLookup val_checkin_group_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValClubTypeIDLookUp val_club_type_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCountryIDLookup val_country_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCurrencyCodeIDLookup val_currency_code_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCurrencyCodeIDLookupCurrencyCode val_currency_code_id_lookup_currency_code,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCWRegionIDLookup val_cw_region_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValEnrollmentTypeIDLookup val_enrollment_type_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValMemberActivityRegionIDLookup val_member_activity_region_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValPreSaleIDlookup val_pre_sale_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValPTRCLAreaIDLookup val_ptrcl_area_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValRegionIDLookup val_region_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValSalesAreaIDlookup val_sales_area_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValStateIDLookup val_state_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.ValTimeZoneIDLookup val_time_zone_id_lookup,
       stage_hash_udwcloudsync_ClubMasterRoster.WorkDayRegionID workday_region_id,
       stage_hash_udwcloudsync_ClubMasterRoster.WorkflowInstanceID workflow_instance_id,
       stage_hash_udwcloudsync_ClubMasterRoster.SpaBiz_StoreNum spabiz_store_num,
       isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ClientID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ITCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MMSClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.OldKronosID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SalesCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValActivityAreaIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCheckInGroupIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValClubTypeIDLookUp,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCountryIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCurrencyCodeIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCurrencyCodeIDLookupCurrencyCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCWRegionIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValEnrollmentTypeIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValMemberActivityRegionIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValPreSaleIDlookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValPTRCLAreaIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValRegionIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValSalesAreaIDlookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValStateIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValTimeZoneIDLookup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.WorkDayRegionID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.WorkflowInstanceID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SpaBiz_StoreNum as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_ClubMasterRoster
 where stage_hash_udwcloudsync_ClubMasterRoster.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_udwcloudsync_club_master_roster records
set @insert_date_time = getdate()
insert into l_udwcloudsync_club_master_roster (
       bk_hash,
       client_id,
       it_code,
       mms_club_id,
       old_kronos_id,
       sales_code,
       val_activity_area_id_lookup,
       val_checkin_group_id_lookup,
       val_club_type_id_lookup,
       val_country_id_lookup,
       val_currency_code_id_lookup,
       val_currency_code_id_lookup_currency_code,
       val_cw_region_id_lookup,
       val_enrollment_type_id_lookup,
       val_member_activity_region_id_lookup,
       val_pre_sale_id_lookup,
       val_ptrcl_area_id_lookup,
       val_region_id_lookup,
       val_sales_area_id_lookup,
       val_state_id_lookup,
       val_time_zone_id_lookup,
       workday_region_id,
       workflow_instance_id,
       spabiz_store_num,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_udwcloudsync_club_master_roster_inserts.bk_hash,
       #l_udwcloudsync_club_master_roster_inserts.client_id,
       #l_udwcloudsync_club_master_roster_inserts.it_code,
       #l_udwcloudsync_club_master_roster_inserts.mms_club_id,
       #l_udwcloudsync_club_master_roster_inserts.old_kronos_id,
       #l_udwcloudsync_club_master_roster_inserts.sales_code,
       #l_udwcloudsync_club_master_roster_inserts.val_activity_area_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_checkin_group_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_club_type_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_country_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_currency_code_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_currency_code_id_lookup_currency_code,
       #l_udwcloudsync_club_master_roster_inserts.val_cw_region_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_enrollment_type_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_member_activity_region_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_pre_sale_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_ptrcl_area_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_region_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_sales_area_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_state_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.val_time_zone_id_lookup,
       #l_udwcloudsync_club_master_roster_inserts.workday_region_id,
       #l_udwcloudsync_club_master_roster_inserts.workflow_instance_id,
       #l_udwcloudsync_club_master_roster_inserts.spabiz_store_num,
       case when l_udwcloudsync_club_master_roster.l_udwcloudsync_club_master_roster_id is null then isnull(#l_udwcloudsync_club_master_roster_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #l_udwcloudsync_club_master_roster_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_udwcloudsync_club_master_roster_inserts
  left join p_udwcloudsync_club_master_roster
    on #l_udwcloudsync_club_master_roster_inserts.bk_hash = p_udwcloudsync_club_master_roster.bk_hash
   and p_udwcloudsync_club_master_roster.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_udwcloudsync_club_master_roster
    on p_udwcloudsync_club_master_roster.bk_hash = l_udwcloudsync_club_master_roster.bk_hash
   and p_udwcloudsync_club_master_roster.l_udwcloudsync_club_master_roster_id = l_udwcloudsync_club_master_roster.l_udwcloudsync_club_master_roster_id
 where l_udwcloudsync_club_master_roster.l_udwcloudsync_club_master_roster_id is null
    or (l_udwcloudsync_club_master_roster.l_udwcloudsync_club_master_roster_id is not null
        and l_udwcloudsync_club_master_roster.dv_hash <> #l_udwcloudsync_club_master_roster_inserts.source_hash)

--calculate hash and lookup to current s_udwcloudsync_club_master_roster
if object_id('tempdb..#s_udwcloudsync_club_master_roster_inserts') is not null drop table #s_udwcloudsync_club_master_roster_inserts
create table #s_udwcloudsync_club_master_roster_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_ClubMasterRoster.bk_hash,
       stage_hash_udwcloudsync_ClubMasterRoster.TwentyFourHourGuard twenty_four_hour_guard,
       stage_hash_udwcloudsync_ClubMasterRoster.ThreeStory three_story,
       stage_hash_udwcloudsync_ClubMasterRoster.AccessType access_type,
       stage_hash_udwcloudsync_ClubMasterRoster.ActivityCenter activity_center,
       stage_hash_udwcloudsync_ClubMasterRoster.AllowJuniorCheckInFlag allow_junior_check_in_flag,
       stage_hash_udwcloudsync_ClubMasterRoster.AppCreatedBy app_created_by,
       stage_hash_udwcloudsync_ClubMasterRoster.AppModifiedBy app_modified_by,
       stage_hash_udwcloudsync_ClubMasterRoster.ApprovalStatus approval_status,
       stage_hash_udwcloudsync_ClubMasterRoster.ApproverComments approver_comments,
       stage_hash_udwcloudsync_ClubMasterRoster.AquaticsDH aquatics_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.Area area,
       stage_hash_udwcloudsync_ClubMasterRoster.AreaDirector area_director,
       stage_hash_udwcloudsync_ClubMasterRoster.AsstGeneralManager asst_general_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.AthleticLTA athletic_lta,
       stage_hash_udwcloudsync_ClubMasterRoster.Attachments attachments,
       stage_hash_udwcloudsync_ClubMasterRoster.BasketballCourts basketball_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.BasketballTrainer basketball_trainer,
       stage_hash_udwcloudsync_ClubMasterRoster.BuldingEngineer bulding_engineer,
       stage_hash_udwcloudsync_ClubMasterRoster.BusinessAdministrator business_administrator,
       stage_hash_udwcloudsync_ClubMasterRoster.Cafe cafe,
       stage_hash_udwcloudsync_ClubMasterRoster.CenterType center_type,
       stage_hash_udwcloudsync_ClubMasterRoster.CheckInGroupLevel check_in_group_level,
       stage_hash_udwcloudsync_ClubMasterRoster.ChildCenter child_center,
       stage_hash_udwcloudsync_ClubMasterRoster.ChildCenterKidsActivitiesDH child_center_kids_activities_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.ChildCenterWeeklyLimit child_center_weekly_limit,
       stage_hash_udwcloudsync_ClubMasterRoster.CIGEndDate cig_end_date,
       stage_hash_udwcloudsync_ClubMasterRoster.CIGStartDate cig_start_date,
       stage_hash_udwcloudsync_ClubMasterRoster.CIGStatus cig_status,
       stage_hash_udwcloudsync_ClubMasterRoster.ClassesOffered classes_offered,
       stage_hash_udwcloudsync_ClubMasterRoster.Clinic clinic,
       stage_hash_udwcloudsync_ClubMasterRoster.ClubAddress club_address,
       stage_hash_udwcloudsync_ClubMasterRoster.ClubID club_id,
       stage_hash_udwcloudsync_ClubMasterRoster.ClubDeactivationDate club_deactivation_date,
       stage_hash_udwcloudsync_ClubMasterRoster.CognosName cognos_name,
       stage_hash_udwcloudsync_ClubMasterRoster.ContentType content_type,
       stage_hash_udwcloudsync_ClubMasterRoster.ContentTypeID content_type_id,
       stage_hash_udwcloudsync_ClubMasterRoster.CopySource copy_source,
       stage_hash_udwcloudsync_ClubMasterRoster.Created created,
       stage_hash_udwcloudsync_ClubMasterRoster.CreatedDate created_date,
       stage_hash_udwcloudsync_ClubMasterRoster.CreatedBy created_by,
       stage_hash_udwcloudsync_ClubMasterRoster.CurrentOperationsStatus current_operations_status,
       stage_hash_udwcloudsync_ClubMasterRoster.CycleStudios cycle_studios,
       stage_hash_udwcloudsync_ClubMasterRoster.DisplayUIFlag display_ui_flag,
       stage_hash_udwcloudsync_ClubMasterRoster.Edit edit,
       stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableEnd edit_menu_table_end,
       stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableStart edit_menu_table_start,
       stage_hash_udwcloudsync_ClubMasterRoster.EffectivePermissionsMask effective_permissions_mask,
       stage_hash_udwcloudsync_ClubMasterRoster.EGroupName egroup_name,
       stage_hash_udwcloudsync_ClubMasterRoster.EmailAlert email_alert,
       stage_hash_udwcloudsync_ClubMasterRoster.EncodedAbsoluteURL encoded_absolute_url,
       stage_hash_udwcloudsync_ClubMasterRoster.EngRCL eng_rcl,
       stage_hash_udwcloudsync_ClubMasterRoster.FacilityOperationsDH facility_operations_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.FacilityTechnician facility_technician,
       stage_hash_udwcloudsync_ClubMasterRoster.FamilySwim family_swim,
       stage_hash_udwcloudsync_ClubMasterRoster.FileName file_name,
       stage_hash_udwcloudsync_ClubMasterRoster.FileType file_type,
       stage_hash_udwcloudsync_ClubMasterRoster.FolderChildCount folder_child_count,
       stage_hash_udwcloudsync_ClubMasterRoster.FormalClubName formal_club_name,
       stage_hash_udwcloudsync_ClubMasterRoster.GeneralManager general_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryAccount gl_cash_entry_account,
       stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCashSubAccount gl_cash_entry_cash_sub_account,
       stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCompanyName gl_cash_entry_company_name,
       stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCreditCardSubAccount gl_cash_entry_credit_card_sub_account,
       stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntryAccount gl_receivables_entry_account,
       stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntryCompanyName gl_receivables_entry_company_name,
       stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntrySubAccount gl_receivables_entry_sub_account,
       stage_hash_udwcloudsync_ClubMasterRoster.GrandPrixTourLocation grand_prix_tour_location,
       stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessDH group_fitness_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessStudios group_fitness_studios,
       stage_hash_udwcloudsync_ClubMasterRoster.GrowthType growth_type,
       stage_hash_udwcloudsync_ClubMasterRoster.[GUID] [guid],
       stage_hash_udwcloudsync_ClubMasterRoster.HasCompanyDestinations has_company_destinations,
       stage_hash_udwcloudsync_ClubMasterRoster.HealthClubIdentifier health_club_identifier,
       stage_hash_udwcloudsync_ClubMasterRoster.HTMLFileType html_file_type,
       stage_hash_udwcloudsync_ClubMasterRoster.HyperionPlanning hyperion_planning,
       stage_hash_udwcloudsync_ClubMasterRoster.[ID] club_master_roster_id,
       stage_hash_udwcloudsync_ClubMasterRoster.IGStoreID ig_store_id,
       stage_hash_udwcloudsync_ClubMasterRoster.IndoorPool indoor_pool,
       stage_hash_udwcloudsync_ClubMasterRoster.IndoorPoolCloseDate indoor_pool_close_date,
       stage_hash_udwcloudsync_ClubMasterRoster.IndoorPoolOpenDate indoor_pool_open_date,
       stage_hash_udwcloudsync_ClubMasterRoster.IndoorTennisCourts indoor_tennis_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.InstanceID instance_id,
       stage_hash_udwcloudsync_ClubMasterRoster.IsCurrentVersion is_current_version,
       stage_hash_udwcloudsync_ClubMasterRoster.ItemChildCount item_child_count,
       stage_hash_udwcloudsync_ClubMasterRoster.ItemType item_type,
       stage_hash_udwcloudsync_ClubMasterRoster.KidsAcademy kids_academy,
       stage_hash_udwcloudsync_ClubMasterRoster.LapPools lap_pools,
       stage_hash_udwcloudsync_ClubMasterRoster.latitide latitide,
       stage_hash_udwcloudsync_ClubMasterRoster.LeasesOwned leases_owned,
       stage_hash_udwcloudsync_ClubMasterRoster.[Level] [level],
       stage_hash_udwcloudsync_ClubMasterRoster.LifeCafeDH life_cafe_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.LifeShop life_shop,
       stage_hash_udwcloudsync_ClubMasterRoster.LifeSpaDH life_spa_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.Longitude longitude,
       stage_hash_udwcloudsync_ClubMasterRoster.MapCenterLatitude map_center_latitude,
       stage_hash_udwcloudsync_ClubMasterRoster.MapCenterLongitude map_center_longitude,
       stage_hash_udwcloudsync_ClubMasterRoster.MapZoomLevel map_zoom_level,
       stage_hash_udwcloudsync_ClubMasterRoster.MarketingClubName marketing_club_name,
       stage_hash_udwcloudsync_ClubMasterRoster.MarketingClubLevel marketing_club_level,
       stage_hash_udwcloudsync_ClubMasterRoster.MarketingMapRegion marketing_map_region,
       stage_hash_udwcloudsync_ClubMasterRoster.MarketingMapXMLStateName marketing_map_xml_state_name,
       stage_hash_udwcloudsync_ClubMasterRoster.MCAStudios mca_studios,
       stage_hash_udwcloudsync_ClubMasterRoster.Medi medi,
       stage_hash_udwcloudsync_ClubMasterRoster.MemberEngagementManager member_engagement_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.MemberServicesDH member_services_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.MembershipLevel membership_level,
       stage_hash_udwcloudsync_ClubMasterRoster.MembershipLevelNeededForTennis membership_level_needed_for_tennis,
       stage_hash_udwcloudsync_ClubMasterRoster.MMSClubID mms_club_id,
       stage_hash_udwcloudsync_ClubMasterRoster.MMSClubName mms_club_name,
       stage_hash_udwcloudsync_ClubMasterRoster.Modified modified,
       stage_hash_udwcloudsync_ClubMasterRoster.LastModified last_modified,
       stage_hash_udwcloudsync_ClubMasterRoster.ModifiedBy modified_by,
       stage_hash_udwcloudsync_ClubMasterRoster.Name_FileLeafRef name_file_leaf_ref,
       stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFilenameNoMenu name_link_file_name_no_menu,
       stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFileName name_link_file_name,
       stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFileName2 name_link_file_name2,
       stage_hash_udwcloudsync_ClubMasterRoster.NumberofSlides number_of_slides,
       stage_hash_udwcloudsync_ClubMasterRoster.NutritionCoach nutrition_coach,
       stage_hash_udwcloudsync_ClubMasterRoster.Open24Hours open_24_hours,
       stage_hash_udwcloudsync_ClubMasterRoster.[Open] [open],
       stage_hash_udwcloudsync_ClubMasterRoster.OpenedDate opened_date,
       stage_hash_udwcloudsync_ClubMasterRoster.OpsRCLSrDH ops_rcl_sr_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.[Order] club_master_roster_order,
       stage_hash_udwcloudsync_ClubMasterRoster.OriginatorID originator_id,
       stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPool outdoor_pool,
       stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPoolCloseDate outdoor_pool_close_date,
       stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPoolOpenDate outdoor_pool_open_date,
       stage_hash_udwcloudsync_ClubMasterRoster.OutDoorTennisCourts outdoor_tennis_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.Owshiddenversion ows_hidden_version,
       stage_hash_udwcloudsync_ClubMasterRoster.ParentsNightOut parents_night_out,
       stage_hash_udwcloudsync_ClubMasterRoster.[Path] [path],
       stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingDH personal_training_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingRCM personal_training_rcm,
       stage_hash_udwcloudsync_ClubMasterRoster.PilatesStudio pilates_studio,
       stage_hash_udwcloudsync_ClubMasterRoster.PreSaleAddress pre_sale_address,
       stage_hash_udwcloudsync_ClubMasterRoster.PriorYearOperationsStatus prior_year_operations_status,
       stage_hash_udwcloudsync_ClubMasterRoster.ProactiveCare proactive_care,
       stage_hash_udwcloudsync_ClubMasterRoster.ProgId prog_id,
       stage_hash_udwcloudsync_ClubMasterRoster.PropertyBag property_bag,
       stage_hash_udwcloudsync_ClubMasterRoster.PTRCMRegion ptrcm_region,
       stage_hash_udwcloudsync_ClubMasterRoster.RacquetballCourts racquet_ball_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.RacquetballLeagueEndDate racquet_ball_league_end_date,
       stage_hash_udwcloudsync_ClubMasterRoster.RacquetballLeagueStartDate racquet_ball_league_start_date,
       stage_hash_udwcloudsync_ClubMasterRoster.Region region,
       stage_hash_udwcloudsync_ClubMasterRoster.RegionalSalesManager regional_sales_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.RegionalVicePresident regional_vice_president,
       stage_hash_udwcloudsync_ClubMasterRoster.Restricted restricted,
       stage_hash_udwcloudsync_ClubMasterRoster.RockWall rock_wall,
       stage_hash_udwcloudsync_ClubMasterRoster.SchoolBreakCamps school_break_camps,
       stage_hash_udwcloudsync_ClubMasterRoster.ScopeId scope_id,
       stage_hash_udwcloudsync_ClubMasterRoster.SelectTitle select_title,
       stage_hash_udwcloudsync_ClubMasterRoster.SeniorGeneralManager senior_general_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.ServerRelativeURL server_relative_url,
       stage_hash_udwcloudsync_ClubMasterRoster.SortType sort_type,
       stage_hash_udwcloudsync_ClubMasterRoster.Spa spa,
       stage_hash_udwcloudsync_ClubMasterRoster.SquareFootage square_footage,
       stage_hash_udwcloudsync_ClubMasterRoster.SquashCourts squash_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.SquashKids squash_kids,
       stage_hash_udwcloudsync_ClubMasterRoster.State state,
       stage_hash_udwcloudsync_ClubMasterRoster.StateAbbreviation state_abbreviation,
       stage_hash_udwcloudsync_ClubMasterRoster.SubnetAddress sub_net_address,
       stage_hash_udwcloudsync_ClubMasterRoster.SummerBreakCamps summer_break_camps,
       stage_hash_udwcloudsync_ClubMasterRoster.TennisCourts tennis_courts,
       stage_hash_udwcloudsync_ClubMasterRoster.TennisDH tennis_dh,
       stage_hash_udwcloudsync_ClubMasterRoster.Title title,
       stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitleNoMenu title_link_title_no_menu,
       stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitle title_link_title,
       stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitle2 title_link_title2,
       stage_hash_udwcloudsync_ClubMasterRoster.TurfField turf_field,
       stage_hash_udwcloudsync_ClubMasterRoster.Type type,
       stage_hash_udwcloudsync_ClubMasterRoster.UIVersion ui_version,
       stage_hash_udwcloudsync_ClubMasterRoster.UniqueFeatures unique_features,
       stage_hash_udwcloudsync_ClubMasterRoster.UniqueID unique_id,
       stage_hash_udwcloudsync_ClubMasterRoster.URLPath url_path,
       stage_hash_udwcloudsync_ClubMasterRoster.ValActivityAreaIDLookupDescription val_activity_area_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCheckInGroupIDLookupDescription val_checkin_group_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValClubTypeIDLookUpDescription val_club_type_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCountryIDLookupTitle val_country_id_lookup_title,
       stage_hash_udwcloudsync_ClubMasterRoster.ValCWRegionIDLookupDescription val_cw_region_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValEnrollmentTypeIDLookupDescription val_enrollment_type_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValMemberActivityRegionIDLookupTitle val_member_activity_region_id_lookup_title,
       stage_hash_udwcloudsync_ClubMasterRoster.ValPreSaleIDlookupDescription val_pre_sale_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValPTRCLAreaIDLookupTitle val_ptrcl_area_id_lookup_title,
       stage_hash_udwcloudsync_ClubMasterRoster.ValRegionIDLookupDescription val_region_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValSalesAreaIDlookupDescription val_sales_area_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValStateIDLookupDescription val_state_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.ValTimeZoneIDLookupDescription val_time_zone_id_lookup_description,
       stage_hash_udwcloudsync_ClubMasterRoster.[Version] [version],
       stage_hash_udwcloudsync_ClubMasterRoster.WaterSlides water_slides,
       stage_hash_udwcloudsync_ClubMasterRoster.Whirpools whirpools,
       stage_hash_udwcloudsync_ClubMasterRoster.WorkFlowVersion workflow_version,
       stage_hash_udwcloudsync_ClubMasterRoster.YogaStudios yoga_studios,
       stage_hash_udwcloudsync_ClubMasterRoster.ZeroDepthEntry zero_depth_entry,
       stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableStart2 edit_menu_table_start2,
       stage_hash_udwcloudsync_ClubMasterRoster.AquaticsRegionalManager aquatics_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.CafeRegionalManager cafe_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.SpaRegionalManager spa_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.RacquetRegionalManager racquet_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingRegionalManager personal_training_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.KidsRegionalManager kids_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.OperationsRegionalManager operationsregionalmanager,
       stage_hash_udwcloudsync_ClubMasterRoster.SalesRegionalManager salesregionalmanager,
       stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessRegionalManager groupfitnessregionalmanager,
       stage_hash_udwcloudsync_ClubMasterRoster.MemberServicesRegionalManager member_services_regional_manager,
       stage_hash_udwcloudsync_ClubMasterRoster.NutritionRegionalManager nutrition_regional_manager,
       isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.TwentyFourHourGuard,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.ThreeStory as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AccessType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ActivityCenter,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.AllowJuniorCheckInFlag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AppCreatedBy,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AppModifiedBy,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ApprovalStatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ApproverComments,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AquaticsDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Area,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AreaDirector,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AsstGeneralManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.AthleticLTA as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Attachments,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.BasketballCourts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.BasketballTrainer,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.BuldingEngineer,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.BusinessAdministrator,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Cafe,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CenterType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CheckInGroupLevel,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ChildCenter,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ChildCenterKidsActivitiesDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.ChildCenterWeeklyLimit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.CIGEndDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.CIGStartDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CIGStatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ClassesOffered,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Clinic,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ClubAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ClubID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.ClubDeactivationDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CognosName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ContentType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ContentTypeID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CopySource,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.Created,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CreatedDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CreatedBy,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CurrentOperationsStatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.CycleStudios as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.DisplayUIFlag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Edit,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableEnd,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableStart,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EffectivePermissionsMask,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EGroupName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EmailAlert,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EncodedAbsoluteURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EngRCL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FacilityOperationsDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FacilityTechnician,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.FamilySwim as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FileName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FileType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FolderChildCount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.FormalClubName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GeneralManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCashSubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCompanyName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLCashEntryCreditCardSubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntryAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntryCompanyName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GLReceivablesEntrySubAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GrandPrixTourLocation,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessStudios as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GrowthType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.[GUID],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.HasCompanyDestinations as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.HealthClubIdentifier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.HTMLFileType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.HyperionPlanning,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.IGStoreID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.IndoorPool,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.IndoorPoolCloseDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.IndoorPoolOpenDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.IndoorTennisCourts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.InstanceID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.IsCurrentVersion as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ItemChildCount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ItemType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.KidsAcademy,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LapPools,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.latitide as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LeasesOwned,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.[Level] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LifeCafeDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LifeShop,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LifeSpaDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.Longitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MapCenterLatitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MapCenterLongitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MapZoomLevel as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MarketingClubName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MarketingClubLevel,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MarketingMapRegion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MarketingMapXMLStateName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MCAStudios as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Medi,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MemberEngagementManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MemberServicesDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MembershipLevel,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MembershipLevelNeededForTennis,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.MMSClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MMSClubName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.Modified,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.LastModified,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ModifiedBy,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Name_FileLeafRef,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFilenameNoMenu,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFileName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Name_LinkFileName2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.NumberofSlides as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.NutritionCoach,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.Open24Hours as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.[Open],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.OpenedDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.OpsRCLSrDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.[Order] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.OriginatorID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPool,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPoolCloseDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.OutDoorPoolOpenDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.OutDoorTennisCourts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.Owshiddenversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.ParentsNightOut as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.[Path],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingRCM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.PilatesStudio as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PreSaleAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PriorYearOperationsStatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ProactiveCare,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ProgId,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PropertyBag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PTRCMRegion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.RacquetballCourts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.RacquetballLeagueEndDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ClubMasterRoster.RacquetballLeagueStartDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Region,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.RegionalSalesManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.RegionalVicePresident,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Restricted,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.RockWall,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SchoolBreakCamps as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ScopeId,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SelectTitle,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SeniorGeneralManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ServerRelativeURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SortType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Spa,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SquareFootage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SquashCourts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SquashKids as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.State,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.StateAbbreviation,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SubnetAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.SummerBreakCamps as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.TennisCourts,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.TennisDH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Title,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitleNoMenu,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitle,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Title_LinkTitle2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.TurfField as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.Type,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.UIVersion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.UniqueFeatures,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.UniqueID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.URLPath,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValActivityAreaIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCheckInGroupIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValClubTypeIDLookUpDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCountryIDLookupTitle,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValCWRegionIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValEnrollmentTypeIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValMemberActivityRegionIDLookupTitle,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValPreSaleIDlookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValPTRCLAreaIDLookupTitle,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValRegionIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValSalesAreaIDlookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValStateIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ValTimeZoneIDLookupDescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.[Version],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.WaterSlides,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.Whirpools as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.WorkFlowVersion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ClubMasterRoster.YogaStudios as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.ZeroDepthEntry,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.EditMenuTableStart2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.AquaticsRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.CafeRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SpaRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.RacquetRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.PersonalTrainingRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.KidsRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.OperationsRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.SalesRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.GroupFitnessRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.MemberServicesRegionalManager,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ClubMasterRoster.NutritionRegionalManager,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_ClubMasterRoster
 where stage_hash_udwcloudsync_ClubMasterRoster.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_udwcloudsync_club_master_roster records
set @insert_date_time = getdate()
insert into s_udwcloudsync_club_master_roster (
       bk_hash,
       twenty_four_hour_guard,
       three_story,
       access_type,
       activity_center,
       allow_junior_check_in_flag,
       app_created_by,
       app_modified_by,
       approval_status,
       approver_comments,
       aquatics_dh,
       area,
       area_director,
       asst_general_manager,
       athletic_lta,
       attachments,
       basketball_courts,
       basketball_trainer,
       bulding_engineer,
       business_administrator,
       cafe,
       center_type,
       check_in_group_level,
       child_center,
       child_center_kids_activities_dh,
       child_center_weekly_limit,
       cig_end_date,
       cig_start_date,
       cig_status,
       classes_offered,
       clinic,
       club_address,
       club_id,
       club_deactivation_date,
       cognos_name,
       content_type,
       content_type_id,
       copy_source,
       created,
       created_date,
       created_by,
       current_operations_status,
       cycle_studios,
       display_ui_flag,
       edit,
       edit_menu_table_end,
       edit_menu_table_start,
       effective_permissions_mask,
       egroup_name,
       email_alert,
       encoded_absolute_url,
       eng_rcl,
       facility_operations_dh,
       facility_technician,
       family_swim,
       file_name,
       file_type,
       folder_child_count,
       formal_club_name,
       general_manager,
       gl_cash_entry_account,
       gl_cash_entry_cash_sub_account,
       gl_cash_entry_company_name,
       gl_cash_entry_credit_card_sub_account,
       gl_receivables_entry_account,
       gl_receivables_entry_company_name,
       gl_receivables_entry_sub_account,
       grand_prix_tour_location,
       group_fitness_dh,
       group_fitness_studios,
       growth_type,
       [guid],
       has_company_destinations,
       health_club_identifier,
       html_file_type,
       hyperion_planning,
       club_master_roster_id,
       ig_store_id,
       indoor_pool,
       indoor_pool_close_date,
       indoor_pool_open_date,
       indoor_tennis_courts,
       instance_id,
       is_current_version,
       item_child_count,
       item_type,
       kids_academy,
       lap_pools,
       latitide,
       leases_owned,
       [level],
       life_cafe_dh,
       life_shop,
       life_spa_dh,
       longitude,
       map_center_latitude,
       map_center_longitude,
       map_zoom_level,
       marketing_club_name,
       marketing_club_level,
       marketing_map_region,
       marketing_map_xml_state_name,
       mca_studios,
       medi,
       member_engagement_manager,
       member_services_dh,
       membership_level,
       membership_level_needed_for_tennis,
       mms_club_id,
       mms_club_name,
       modified,
       last_modified,
       modified_by,
       name_file_leaf_ref,
       name_link_file_name_no_menu,
       name_link_file_name,
       name_link_file_name2,
       number_of_slides,
       nutrition_coach,
       open_24_hours,
       [open],
       opened_date,
       ops_rcl_sr_dh,
       club_master_roster_order,
       originator_id,
       outdoor_pool,
       outdoor_pool_close_date,
       outdoor_pool_open_date,
       outdoor_tennis_courts,
       ows_hidden_version,
       parents_night_out,
       [path],
       personal_training_dh,
       personal_training_rcm,
       pilates_studio,
       pre_sale_address,
       prior_year_operations_status,
       proactive_care,
       prog_id,
       property_bag,
       ptrcm_region,
       racquet_ball_courts,
       racquet_ball_league_end_date,
       racquet_ball_league_start_date,
       region,
       regional_sales_manager,
       regional_vice_president,
       restricted,
       rock_wall,
       school_break_camps,
       scope_id,
       select_title,
       senior_general_manager,
       server_relative_url,
       sort_type,
       spa,
       square_footage,
       squash_courts,
       squash_kids,
       state,
       state_abbreviation,
       sub_net_address,
       summer_break_camps,
       tennis_courts,
       tennis_dh,
       title,
       title_link_title_no_menu,
       title_link_title,
       title_link_title2,
       turf_field,
       type,
       ui_version,
       unique_features,
       unique_id,
       url_path,
       val_activity_area_id_lookup_description,
       val_checkin_group_id_lookup_description,
       val_club_type_id_lookup_description,
       val_country_id_lookup_title,
       val_cw_region_id_lookup_description,
       val_enrollment_type_id_lookup_description,
       val_member_activity_region_id_lookup_title,
       val_pre_sale_id_lookup_description,
       val_ptrcl_area_id_lookup_title,
       val_region_id_lookup_description,
       val_sales_area_id_lookup_description,
       val_state_id_lookup_description,
       val_time_zone_id_lookup_description,
       [version],
       water_slides,
       whirpools,
       workflow_version,
       yoga_studios,
       zero_depth_entry,
       edit_menu_table_start2,
       aquatics_regional_manager,
       cafe_regional_manager,
       spa_regional_manager,
       racquet_regional_manager,
       personal_training_regional_manager,
       kids_regional_manager,
       operationsregionalmanager,
       salesregionalmanager,
       groupfitnessregionalmanager,
       member_services_regional_manager,
       nutrition_regional_manager,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_udwcloudsync_club_master_roster_inserts.bk_hash,
       #s_udwcloudsync_club_master_roster_inserts.twenty_four_hour_guard,
       #s_udwcloudsync_club_master_roster_inserts.three_story,
       #s_udwcloudsync_club_master_roster_inserts.access_type,
       #s_udwcloudsync_club_master_roster_inserts.activity_center,
       #s_udwcloudsync_club_master_roster_inserts.allow_junior_check_in_flag,
       #s_udwcloudsync_club_master_roster_inserts.app_created_by,
       #s_udwcloudsync_club_master_roster_inserts.app_modified_by,
       #s_udwcloudsync_club_master_roster_inserts.approval_status,
       #s_udwcloudsync_club_master_roster_inserts.approver_comments,
       #s_udwcloudsync_club_master_roster_inserts.aquatics_dh,
       #s_udwcloudsync_club_master_roster_inserts.area,
       #s_udwcloudsync_club_master_roster_inserts.area_director,
       #s_udwcloudsync_club_master_roster_inserts.asst_general_manager,
       #s_udwcloudsync_club_master_roster_inserts.athletic_lta,
       #s_udwcloudsync_club_master_roster_inserts.attachments,
       #s_udwcloudsync_club_master_roster_inserts.basketball_courts,
       #s_udwcloudsync_club_master_roster_inserts.basketball_trainer,
       #s_udwcloudsync_club_master_roster_inserts.bulding_engineer,
       #s_udwcloudsync_club_master_roster_inserts.business_administrator,
       #s_udwcloudsync_club_master_roster_inserts.cafe,
       #s_udwcloudsync_club_master_roster_inserts.center_type,
       #s_udwcloudsync_club_master_roster_inserts.check_in_group_level,
       #s_udwcloudsync_club_master_roster_inserts.child_center,
       #s_udwcloudsync_club_master_roster_inserts.child_center_kids_activities_dh,
       #s_udwcloudsync_club_master_roster_inserts.child_center_weekly_limit,
       #s_udwcloudsync_club_master_roster_inserts.cig_end_date,
       #s_udwcloudsync_club_master_roster_inserts.cig_start_date,
       #s_udwcloudsync_club_master_roster_inserts.cig_status,
       #s_udwcloudsync_club_master_roster_inserts.classes_offered,
       #s_udwcloudsync_club_master_roster_inserts.clinic,
       #s_udwcloudsync_club_master_roster_inserts.club_address,
       #s_udwcloudsync_club_master_roster_inserts.club_id,
       #s_udwcloudsync_club_master_roster_inserts.club_deactivation_date,
       #s_udwcloudsync_club_master_roster_inserts.cognos_name,
       #s_udwcloudsync_club_master_roster_inserts.content_type,
       #s_udwcloudsync_club_master_roster_inserts.content_type_id,
       #s_udwcloudsync_club_master_roster_inserts.copy_source,
       #s_udwcloudsync_club_master_roster_inserts.created,
       #s_udwcloudsync_club_master_roster_inserts.created_date,
       #s_udwcloudsync_club_master_roster_inserts.created_by,
       #s_udwcloudsync_club_master_roster_inserts.current_operations_status,
       #s_udwcloudsync_club_master_roster_inserts.cycle_studios,
       #s_udwcloudsync_club_master_roster_inserts.display_ui_flag,
       #s_udwcloudsync_club_master_roster_inserts.edit,
       #s_udwcloudsync_club_master_roster_inserts.edit_menu_table_end,
       #s_udwcloudsync_club_master_roster_inserts.edit_menu_table_start,
       #s_udwcloudsync_club_master_roster_inserts.effective_permissions_mask,
       #s_udwcloudsync_club_master_roster_inserts.egroup_name,
       #s_udwcloudsync_club_master_roster_inserts.email_alert,
       #s_udwcloudsync_club_master_roster_inserts.encoded_absolute_url,
       #s_udwcloudsync_club_master_roster_inserts.eng_rcl,
       #s_udwcloudsync_club_master_roster_inserts.facility_operations_dh,
       #s_udwcloudsync_club_master_roster_inserts.facility_technician,
       #s_udwcloudsync_club_master_roster_inserts.family_swim,
       #s_udwcloudsync_club_master_roster_inserts.file_name,
       #s_udwcloudsync_club_master_roster_inserts.file_type,
       #s_udwcloudsync_club_master_roster_inserts.folder_child_count,
       #s_udwcloudsync_club_master_roster_inserts.formal_club_name,
       #s_udwcloudsync_club_master_roster_inserts.general_manager,
       #s_udwcloudsync_club_master_roster_inserts.gl_cash_entry_account,
       #s_udwcloudsync_club_master_roster_inserts.gl_cash_entry_cash_sub_account,
       #s_udwcloudsync_club_master_roster_inserts.gl_cash_entry_company_name,
       #s_udwcloudsync_club_master_roster_inserts.gl_cash_entry_credit_card_sub_account,
       #s_udwcloudsync_club_master_roster_inserts.gl_receivables_entry_account,
       #s_udwcloudsync_club_master_roster_inserts.gl_receivables_entry_company_name,
       #s_udwcloudsync_club_master_roster_inserts.gl_receivables_entry_sub_account,
       #s_udwcloudsync_club_master_roster_inserts.grand_prix_tour_location,
       #s_udwcloudsync_club_master_roster_inserts.group_fitness_dh,
       #s_udwcloudsync_club_master_roster_inserts.group_fitness_studios,
       #s_udwcloudsync_club_master_roster_inserts.growth_type,
       #s_udwcloudsync_club_master_roster_inserts.[guid],
       #s_udwcloudsync_club_master_roster_inserts.has_company_destinations,
       #s_udwcloudsync_club_master_roster_inserts.health_club_identifier,
       #s_udwcloudsync_club_master_roster_inserts.html_file_type,
       #s_udwcloudsync_club_master_roster_inserts.hyperion_planning,
       #s_udwcloudsync_club_master_roster_inserts.club_master_roster_id,
       #s_udwcloudsync_club_master_roster_inserts.ig_store_id,
       #s_udwcloudsync_club_master_roster_inserts.indoor_pool,
       #s_udwcloudsync_club_master_roster_inserts.indoor_pool_close_date,
       #s_udwcloudsync_club_master_roster_inserts.indoor_pool_open_date,
       #s_udwcloudsync_club_master_roster_inserts.indoor_tennis_courts,
       #s_udwcloudsync_club_master_roster_inserts.instance_id,
       #s_udwcloudsync_club_master_roster_inserts.is_current_version,
       #s_udwcloudsync_club_master_roster_inserts.item_child_count,
       #s_udwcloudsync_club_master_roster_inserts.item_type,
       #s_udwcloudsync_club_master_roster_inserts.kids_academy,
       #s_udwcloudsync_club_master_roster_inserts.lap_pools,
       #s_udwcloudsync_club_master_roster_inserts.latitide,
       #s_udwcloudsync_club_master_roster_inserts.leases_owned,
       #s_udwcloudsync_club_master_roster_inserts.[level],
       #s_udwcloudsync_club_master_roster_inserts.life_cafe_dh,
       #s_udwcloudsync_club_master_roster_inserts.life_shop,
       #s_udwcloudsync_club_master_roster_inserts.life_spa_dh,
       #s_udwcloudsync_club_master_roster_inserts.longitude,
       #s_udwcloudsync_club_master_roster_inserts.map_center_latitude,
       #s_udwcloudsync_club_master_roster_inserts.map_center_longitude,
       #s_udwcloudsync_club_master_roster_inserts.map_zoom_level,
       #s_udwcloudsync_club_master_roster_inserts.marketing_club_name,
       #s_udwcloudsync_club_master_roster_inserts.marketing_club_level,
       #s_udwcloudsync_club_master_roster_inserts.marketing_map_region,
       #s_udwcloudsync_club_master_roster_inserts.marketing_map_xml_state_name,
       #s_udwcloudsync_club_master_roster_inserts.mca_studios,
       #s_udwcloudsync_club_master_roster_inserts.medi,
       #s_udwcloudsync_club_master_roster_inserts.member_engagement_manager,
       #s_udwcloudsync_club_master_roster_inserts.member_services_dh,
       #s_udwcloudsync_club_master_roster_inserts.membership_level,
       #s_udwcloudsync_club_master_roster_inserts.membership_level_needed_for_tennis,
       #s_udwcloudsync_club_master_roster_inserts.mms_club_id,
       #s_udwcloudsync_club_master_roster_inserts.mms_club_name,
       #s_udwcloudsync_club_master_roster_inserts.modified,
       #s_udwcloudsync_club_master_roster_inserts.last_modified,
       #s_udwcloudsync_club_master_roster_inserts.modified_by,
       #s_udwcloudsync_club_master_roster_inserts.name_file_leaf_ref,
       #s_udwcloudsync_club_master_roster_inserts.name_link_file_name_no_menu,
       #s_udwcloudsync_club_master_roster_inserts.name_link_file_name,
       #s_udwcloudsync_club_master_roster_inserts.name_link_file_name2,
       #s_udwcloudsync_club_master_roster_inserts.number_of_slides,
       #s_udwcloudsync_club_master_roster_inserts.nutrition_coach,
       #s_udwcloudsync_club_master_roster_inserts.open_24_hours,
       #s_udwcloudsync_club_master_roster_inserts.[open],
       #s_udwcloudsync_club_master_roster_inserts.opened_date,
       #s_udwcloudsync_club_master_roster_inserts.ops_rcl_sr_dh,
       #s_udwcloudsync_club_master_roster_inserts.club_master_roster_order,
       #s_udwcloudsync_club_master_roster_inserts.originator_id,
       #s_udwcloudsync_club_master_roster_inserts.outdoor_pool,
       #s_udwcloudsync_club_master_roster_inserts.outdoor_pool_close_date,
       #s_udwcloudsync_club_master_roster_inserts.outdoor_pool_open_date,
       #s_udwcloudsync_club_master_roster_inserts.outdoor_tennis_courts,
       #s_udwcloudsync_club_master_roster_inserts.ows_hidden_version,
       #s_udwcloudsync_club_master_roster_inserts.parents_night_out,
       #s_udwcloudsync_club_master_roster_inserts.[path],
       #s_udwcloudsync_club_master_roster_inserts.personal_training_dh,
       #s_udwcloudsync_club_master_roster_inserts.personal_training_rcm,
       #s_udwcloudsync_club_master_roster_inserts.pilates_studio,
       #s_udwcloudsync_club_master_roster_inserts.pre_sale_address,
       #s_udwcloudsync_club_master_roster_inserts.prior_year_operations_status,
       #s_udwcloudsync_club_master_roster_inserts.proactive_care,
       #s_udwcloudsync_club_master_roster_inserts.prog_id,
       #s_udwcloudsync_club_master_roster_inserts.property_bag,
       #s_udwcloudsync_club_master_roster_inserts.ptrcm_region,
       #s_udwcloudsync_club_master_roster_inserts.racquet_ball_courts,
       #s_udwcloudsync_club_master_roster_inserts.racquet_ball_league_end_date,
       #s_udwcloudsync_club_master_roster_inserts.racquet_ball_league_start_date,
       #s_udwcloudsync_club_master_roster_inserts.region,
       #s_udwcloudsync_club_master_roster_inserts.regional_sales_manager,
       #s_udwcloudsync_club_master_roster_inserts.regional_vice_president,
       #s_udwcloudsync_club_master_roster_inserts.restricted,
       #s_udwcloudsync_club_master_roster_inserts.rock_wall,
       #s_udwcloudsync_club_master_roster_inserts.school_break_camps,
       #s_udwcloudsync_club_master_roster_inserts.scope_id,
       #s_udwcloudsync_club_master_roster_inserts.select_title,
       #s_udwcloudsync_club_master_roster_inserts.senior_general_manager,
       #s_udwcloudsync_club_master_roster_inserts.server_relative_url,
       #s_udwcloudsync_club_master_roster_inserts.sort_type,
       #s_udwcloudsync_club_master_roster_inserts.spa,
       #s_udwcloudsync_club_master_roster_inserts.square_footage,
       #s_udwcloudsync_club_master_roster_inserts.squash_courts,
       #s_udwcloudsync_club_master_roster_inserts.squash_kids,
       #s_udwcloudsync_club_master_roster_inserts.state,
       #s_udwcloudsync_club_master_roster_inserts.state_abbreviation,
       #s_udwcloudsync_club_master_roster_inserts.sub_net_address,
       #s_udwcloudsync_club_master_roster_inserts.summer_break_camps,
       #s_udwcloudsync_club_master_roster_inserts.tennis_courts,
       #s_udwcloudsync_club_master_roster_inserts.tennis_dh,
       #s_udwcloudsync_club_master_roster_inserts.title,
       #s_udwcloudsync_club_master_roster_inserts.title_link_title_no_menu,
       #s_udwcloudsync_club_master_roster_inserts.title_link_title,
       #s_udwcloudsync_club_master_roster_inserts.title_link_title2,
       #s_udwcloudsync_club_master_roster_inserts.turf_field,
       #s_udwcloudsync_club_master_roster_inserts.type,
       #s_udwcloudsync_club_master_roster_inserts.ui_version,
       #s_udwcloudsync_club_master_roster_inserts.unique_features,
       #s_udwcloudsync_club_master_roster_inserts.unique_id,
       #s_udwcloudsync_club_master_roster_inserts.url_path,
       #s_udwcloudsync_club_master_roster_inserts.val_activity_area_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_checkin_group_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_club_type_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_country_id_lookup_title,
       #s_udwcloudsync_club_master_roster_inserts.val_cw_region_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_enrollment_type_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_member_activity_region_id_lookup_title,
       #s_udwcloudsync_club_master_roster_inserts.val_pre_sale_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_ptrcl_area_id_lookup_title,
       #s_udwcloudsync_club_master_roster_inserts.val_region_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_sales_area_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_state_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.val_time_zone_id_lookup_description,
       #s_udwcloudsync_club_master_roster_inserts.[version],
       #s_udwcloudsync_club_master_roster_inserts.water_slides,
       #s_udwcloudsync_club_master_roster_inserts.whirpools,
       #s_udwcloudsync_club_master_roster_inserts.workflow_version,
       #s_udwcloudsync_club_master_roster_inserts.yoga_studios,
       #s_udwcloudsync_club_master_roster_inserts.zero_depth_entry,
       #s_udwcloudsync_club_master_roster_inserts.edit_menu_table_start2,
       #s_udwcloudsync_club_master_roster_inserts.aquatics_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.cafe_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.spa_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.racquet_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.personal_training_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.kids_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.operationsregionalmanager,
       #s_udwcloudsync_club_master_roster_inserts.salesregionalmanager,
       #s_udwcloudsync_club_master_roster_inserts.groupfitnessregionalmanager,
       #s_udwcloudsync_club_master_roster_inserts.member_services_regional_manager,
       #s_udwcloudsync_club_master_roster_inserts.nutrition_regional_manager,
       case when s_udwcloudsync_club_master_roster.s_udwcloudsync_club_master_roster_id is null then isnull(#s_udwcloudsync_club_master_roster_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #s_udwcloudsync_club_master_roster_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_udwcloudsync_club_master_roster_inserts
  left join p_udwcloudsync_club_master_roster
    on #s_udwcloudsync_club_master_roster_inserts.bk_hash = p_udwcloudsync_club_master_roster.bk_hash
   and p_udwcloudsync_club_master_roster.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_udwcloudsync_club_master_roster
    on p_udwcloudsync_club_master_roster.bk_hash = s_udwcloudsync_club_master_roster.bk_hash
   and p_udwcloudsync_club_master_roster.s_udwcloudsync_club_master_roster_id = s_udwcloudsync_club_master_roster.s_udwcloudsync_club_master_roster_id
 where s_udwcloudsync_club_master_roster.s_udwcloudsync_club_master_roster_id is null
    or (s_udwcloudsync_club_master_roster.s_udwcloudsync_club_master_roster_id is not null
        and s_udwcloudsync_club_master_roster.dv_hash <> #s_udwcloudsync_club_master_roster_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_udwcloudsync_club_master_roster @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_udwcloudsync_club_master_roster @current_dv_batch_id

end
