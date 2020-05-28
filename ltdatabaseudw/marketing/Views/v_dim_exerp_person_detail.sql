CREATE VIEW [marketing].[v_dim_exerp_person_detail]
AS select d_exerp_person_detail.person_id person_id,
       d_exerp_person_detail.address_1 address_1,
       d_exerp_person_detail.address_2 address_2,
       d_exerp_person_detail.address_3 address_3,
       d_exerp_person_detail.center_id center_id,
       d_exerp_person_detail.dim_club_key dim_club_key,
       d_exerp_person_detail.dim_mms_member_key dim_mms_member_key,
       d_exerp_person_detail.email email,
       d_exerp_person_detail.ets ets,
       d_exerp_person_detail.first_name first_name,
       d_exerp_person_detail.full_name full_name,
       d_exerp_person_detail.home_phone home_phone,
       d_exerp_person_detail.last_name last_name,
       d_exerp_person_detail.mobile_phone mobile_phone,
       d_exerp_person_detail.work_phone work_phone
  from dbo.d_exerp_person_detail;