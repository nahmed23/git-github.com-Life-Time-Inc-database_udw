CREATE VIEW [marketing].[v_dim_fitmetrix_location_resource] AS select d_fitmetrix_api_facility_location_id_resources.dim_fitmetrix_location_resource_key dim_fitmetrix_location_resource_key,
       d_fitmetrix_api_facility_location_id_resources.facility_location_resource_id facility_location_resource_id,
       d_fitmetrix_api_facility_location_id_resources.boss_resource_id boss_resource_id,
       d_fitmetrix_api_facility_location_id_resources.dim_fitmetrix_location_key dim_fitmetrix_location_key,
       d_fitmetrix_api_facility_location_id_resources.max_capacity max_capacity,
       d_fitmetrix_api_facility_location_id_resources.resource_name resource_name
  from dbo.d_fitmetrix_api_facility_location_id_resources;