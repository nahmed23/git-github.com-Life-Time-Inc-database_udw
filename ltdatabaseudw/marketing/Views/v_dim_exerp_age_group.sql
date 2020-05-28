CREATE VIEW [marketing].[v_dim_exerp_age_group]
AS select d_exerp_age_group.dim_exerp_age_group_key dim_exerp_age_group_key,
       d_exerp_age_group.age_group_id age_group_id,
       d_exerp_age_group.age_group_name age_group_name,
       d_exerp_age_group.age_group_state age_group_state,
       d_exerp_age_group.external_id external_id,
       d_exerp_age_group.maximum_age maximum_age,
       d_exerp_age_group.minimum_age minimum_age,
       d_exerp_age_group.strict_age_limit strict_age_limit
  from dbo.d_exerp_age_group;