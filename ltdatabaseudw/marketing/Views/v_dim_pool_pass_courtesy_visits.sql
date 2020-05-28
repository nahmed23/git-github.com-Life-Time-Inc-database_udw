CREATE VIEW [marketing].[v_dim_pool_pass_courtesy_visits]
AS select d_pool_pass_courtesy_visits.courtesy_visits_id courtesy_visits_id,
       d_pool_pass_courtesy_visits.club_id club_id,
       d_pool_pass_courtesy_visits.created_date created_date,
       d_pool_pass_courtesy_visits.created_dim_date_key created_dim_date_key,
       d_pool_pass_courtesy_visits.created_dim_time_key created_dim_time_key,
       d_pool_pass_courtesy_visits.dim_club_key dim_club_key,
       d_pool_pass_courtesy_visits.employee_party_id employee_party_id,
       d_pool_pass_courtesy_visits.member_party_id member_party_id,
       d_pool_pass_courtesy_visits.updated_date updated_date,
       d_pool_pass_courtesy_visits.updated_dim_date_key updated_dim_date_key,
       d_pool_pass_courtesy_visits.updated_dim_time_key updated_dim_time_key
  from dbo.d_pool_pass_courtesy_visits;