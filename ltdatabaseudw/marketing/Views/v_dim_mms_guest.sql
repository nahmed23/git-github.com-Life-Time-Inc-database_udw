CREATE VIEW [marketing].[v_dim_mms_guest] AS select d_mms_guest.dim_club_guest_key dim_club_guest_key,
       d_mms_guest.guest_id guest_id,
       d_mms_guest.first_name first_name,
       d_mms_guest.last_name last_name
  from dbo.d_mms_guest;