CREATE PROC [reporting].[proc_PromptClubByRegion] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 ------ JIRA : REP-5944
 ------ Execution Sample: EXEC [reporting].[proc_PromptClubByRegion]


SELECT MMSREgion.description as MMSRegionName,
       loc.club_id as MMSClubID,
       loc.club_name as ClubName,
       loc.club_code as ClubCode,
       loc.gl_club_id as GLClubID,
       case when loc.club_status ='Presale' then 'Y' else 'N' end as PreSaleFlag,
       MemActRegion.description as MemberActivitiesRegionName,
       PTRCLAreaName.description as PTRCLAreaName,
       SalesAreaName.description as SalesAreaName,
       club_code + ' - ' + club_name as ClubCodeDashClubName,
       loc.dim_club_key
FROM marketing.v_dim_club loc
LEFT JOIN marketing.v_dim_description MMSREgion on loc.region_dim_description_key  = MMSREgion.dim_description_key
LEFT JOIN marketing.v_dim_description MemActRegion on loc.member_activities_region_dim_description_key  = MemActRegion.dim_description_key
LEFT JOIN marketing.v_dim_description PTRCLAreaName on loc.pt_rcl_area_dim_description_key  = PTRCLAreaName.dim_description_key
LEFT JOIN marketing.v_dim_description SalesAreaName on loc.sales_area_dim_description_key  = SalesAreaName.dim_description_key
WHERE loc.dim_club_key not in ('-997','-998','-999') and MMSREgion.description is not null

END
