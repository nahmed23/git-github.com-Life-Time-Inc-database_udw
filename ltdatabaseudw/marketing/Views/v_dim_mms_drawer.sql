CREATE VIEW [marketing].[v_dim_mms_drawer] AS select d_mms_drawer.dim_mms_drawer_key dim_mms_drawer_key,
       d_mms_drawer.drawer_id drawer_id,
       d_mms_drawer.club_id club_id,
       d_mms_drawer.description description,
       d_mms_drawer.dim_club_key dim_club_key,
       d_mms_drawer.locked_flag locked_flag,
       d_mms_drawer.starting_cash_amount starting_cash_amount
  from dbo.d_mms_drawer;