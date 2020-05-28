CREATE VIEW [marketing].[v_dim_crm_team]
AS select d_crmcloudsync_team.dim_crm_team_key dim_crm_team_key,
       d_crmcloudsync_team.team_id team_id,
       d_crmcloudsync_team.email_address email_address,
       d_crmcloudsync_team.ltf_telephone_1 ltf_telephone_1,
       d_crmcloudsync_team.name name
  from dbo.d_crmcloudsync_team;