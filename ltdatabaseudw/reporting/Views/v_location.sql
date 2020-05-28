CREATE VIEW [reporting].[v_location]
AS SELECT 
	club.dim_club_key
	,club.club_id
	,club.club_name
	,club.city
	,club.state as 'state_abbreviation'
	,club.state_or_province
	,club.address_line_1
	,club.club_code
	,club.club_hours
	,open_date.calendar_date as 'club_open_date'
	,close_date.calendar_date as 'club_close_date'
	,club.club_membership_level
	,cmr.[Tennis Membership Level] as 'tennis_membership_level'
	,club.marketing_club_level
	,club.marketing_map_region
	,club.marketing_name
	,club.club_status
	,CASE WHEN club.club_name LIKE '%work%' THEN 'LT Work'
		ELSE club.club_type
		 END as 'club_type'
	,club.country
	,club.latitude
	,club.longitude
	,member_activities.description as 'member_activities_region'
	,club.postal_code
	,pt_rcl.description as 'pt_rcl_area'
	,region.description as 'region'
	,sales_area.description as 'sales_area'
	,cmr.area
	,club.general_manager

	-- PPI
	--,club.member_services_manager   
	--,region_sales_lead.full_name as 'regional_sales_lead'
	--,rvp.full_name as 'regional_vice_president'
	--,regional_manager.full_name as 'club_regional_manager'
	--,area_director.full_name as 'area_director'

	,club.workday_region
	,cmr.Workday_Company
	,cmr.[current operations status] as current_operations_status

	--PPI
	--,cmr.personaltraining_rm as 'personal_training_rm'
	--,cmr.aquatics_rm
	--,cmr.racquet_rm
	--,cmr.kids_rm
	--,cmr.memberservices_rm as 'member_services_rm'
	--,cmr.spa_rm
	--,cmr.spabiz_storenum as 'spa_biz_store_num'
	--,cmr.groupfitness_rm as 'group_fitness_rm'
	--,cmr.cafe_rm
	--,cmr.PT_Area_Lead

	,cmr.hyperion_id
	,cmr.[FP comp status] as 'FP_comp_status'
	,cmr.[13M Comp Start] as '13m_comp_start'
	,cmr.[37M Comp Start] as '37m_comp_start'
	,cmr.medallia_id
	,cmr.humanity_id
	,cmr.[E-group name] as e_group_name
	,club.domain_name_prefix
	,club.content_type
	,cmr.[Athletic (LTA)] as 'athletic_lta'
	, CASE WHEN club.club_id IN ('263') THEN CAST('2020-05-08' as date)
			WHEN club.club_id IN ('199','154') THEN CAST('2020-05-15' as date)
			WHEN club.club_id IN ('136','138','139','140','142','143','146','147','152','153','179','180','185','190','196','201','206','230','266','268','279','272','271','270') THEN CAST('2020-05-18' as date)
			WHEN club.club_id IN ('132','137','155','156','157','158','169','178','182','184','198','208','213','232','238') THEN CAST('2020-05-25' as date)
			WHEN club.club_id IN ('40','159','162','195','223','224','284') THEN CAST('2020-05-26' as date)
			ELSE NULL
		END AS club_re_open_date


	



FROM dbo.dim_club club
LEFT JOIN dbo.dim_description region
ON club.region_dim_description_key=region.dim_description_key
LEFT JOIN dbo.dim_description member_activities
ON club.member_activities_region_dim_description_key=member_activities.dim_description_key
LEFT JOIN dbo.dim_description pt_rcl
ON club.pt_rcl_area_dim_description_key=pt_rcl.dim_description_key
LEFT JOIN dbo.dim_description sales_area
ON club.sales_area_dim_description_key=sales_area.dim_description_key
LEFT JOIN dbo.d_crmcloudsync_system_user region_sales_lead
ON club.regional_sales_lead_dim_crm_system_user_key=region_sales_lead.dim_crm_system_user_key
LEFT JOIN dbo.d_crmcloudsync_system_user rvp
ON club.regional_vice_president_dim_crm_system_user_key=rvp.dim_crm_system_user_key
LEFT JOIN dbo.d_crmcloudsync_system_user regional_manager
ON club.club_regional_manager_dim_crm_system_user_key=regional_manager.dim_crm_system_user_key
LEFT JOIN dbo.d_crmcloudsync_system_user area_director
ON club.area_director_dim_crm_system_user_key=area_director.dim_crm_system_user_key
LEFT JOIN dbo.dim_date open_date
ON club.club_open_dim_date_key=open_date.dim_date_key
LEFT JOIN dbo.dim_date close_date
ON club.club_close_dim_date_key=close_date.dim_date_key
--LEFT JOINing club master roster view. This needs to get updated with the CMR table once it is put into production
LEFT JOIN sandbox_ebi.v_cmr cmr   --FOR PROD
ON cmr.[MMS Club ID]=club.club_id;