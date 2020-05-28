CREATE VIEW [marketing].[v_dim_exerp_resource]
AS select d_exerp_resource.resource_id resource_id,
       d_exerp_resource.access_group_id access_group_id,
       d_exerp_resource.comment comment,
       d_exerp_resource.d_exerp_center_bk_hash d_exerp_center_bk_hash,
       d_exerp_resource.external_id external_id,
       d_exerp_resource.resource_access_group_name resource_access_group_name,
       d_exerp_resource.resource_name resource_name,
       d_exerp_resource.resource_state resource_state,
       d_exerp_resource.resource_type resource_type,
       d_exerp_resource.show_calendar show_calendar
  from dbo.d_exerp_resource;