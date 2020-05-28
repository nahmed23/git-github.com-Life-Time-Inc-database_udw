CREATE VIEW [marketing].[v_dim_club_bridge_dim_mms_product]
AS select d_mms_club_product.dim_club_bridge_dim_mms_product_key dim_club_bridge_dim_mms_product_key,
       d_mms_club_product.club_product_id club_product_id,
       d_mms_club_product.dim_club_key dim_club_key,
       d_mms_club_product.dim_mms_product_key dim_mms_product_key,
       d_mms_club_product.price price,
       d_mms_club_product.sold_in_pk_flag sold_in_pk_flag,
       d_mms_club_product.val_commissionable_id val_commissionable_id
  from dbo.d_mms_club_product;