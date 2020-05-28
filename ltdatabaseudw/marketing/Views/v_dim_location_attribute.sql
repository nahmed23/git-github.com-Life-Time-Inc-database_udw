CREATE VIEW [marketing].[v_dim_location_attribute]
AS select dim_location_attribute.attribute_value attribute_value,
       dim_location_attribute.business_key business_key,
       dim_location_attribute.business_source_name business_source_name,
       dim_location_attribute.created_by created_by,
       dim_location_attribute.created_dim_date_key created_dim_date_key,
       dim_location_attribute.deleted_by deleted_by,
       dim_location_attribute.deleted_dim_date_key deleted_dim_date_key,
       dim_location_attribute.dim_location_attribute_key dim_location_attribute_key,
       dim_location_attribute.dim_location_key dim_location_key,
       dim_location_attribute.location_attribute_type_display_name location_attribute_type_display_name,
       dim_location_attribute.location_attribute_type_group_display_name location_attribute_type_group_display_name,
       dim_location_attribute.location_attribute_type_group_name location_attribute_type_group_name,
       dim_location_attribute.location_attribute_type_name location_attribute_type_name,
       dim_location_attribute.managed_by_udw_flag managed_by_udw_flag,
       dim_location_attribute.updated_by updated_by,
       dim_location_attribute.updated_dim_date_key updated_dim_date_key
  from dbo.dim_location_attribute;