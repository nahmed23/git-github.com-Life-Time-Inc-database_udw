CREATE VIEW [marketing].[v_dim_ltfeb_ltf_user_identity]
AS select d_ltfeb_ltf_user_identity.party_id party_id,
       d_ltfeb_ltf_user_identity.ltf_user_name ltf_user_name
  from dbo.d_ltfeb_ltf_user_identity;