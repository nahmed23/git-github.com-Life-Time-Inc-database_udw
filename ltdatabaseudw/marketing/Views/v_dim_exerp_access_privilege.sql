CREATE VIEW [marketing].[v_dim_exerp_access_privilege]
AS select d_exerp_access_privilege.access_privilege_id access_privilege_id,
       d_exerp_access_privilege.access_group_id access_group_id,
       d_exerp_access_privilege.access_privilege_scope_id access_privilege_scope_id,
       d_exerp_access_privilege.access_privilege_scope_type access_privilege_scope_type,
       d_exerp_access_privilege.d_exerp_access_group_bk_hash d_exerp_access_group_bk_hash,
       d_exerp_access_privilege.d_exerp_access_privilege_scope_bk_hash d_exerp_access_privilege_scope_bk_hash,
       d_exerp_access_privilege.dim_exerp_privilege_set_key dim_exerp_privilege_set_key,
       d_exerp_access_privilege.privilege_set_id privilege_set_id
  from dbo.d_exerp_access_privilege;