CREATE VIEW [marketing].[v_dim_spabiz_gift_certificate_type] AS select d_dim_spabiz_gift_certificate_type.dim_spabiz_gift_certificate_type_key dim_spabiz_gift_certificate_type_key,
       d_dim_spabiz_gift_certificate_type.gift_id gift_id,
       d_dim_spabiz_gift_certificate_type.store_number store_number,
       d_dim_spabiz_gift_certificate_type.d_dim_spabiz_gift_certificate_type_id d_dim_spabiz_gift_certificate_type_id,
       d_dim_spabiz_gift_certificate_type.deleted_date_time deleted_date_time,
       d_dim_spabiz_gift_certificate_type.deleted_flag deleted_flag,
       d_dim_spabiz_gift_certificate_type.dim_spabiz_store_key dim_spabiz_store_key,
       d_dim_spabiz_gift_certificate_type.edit_date_time edit_date_time,
       d_dim_spabiz_gift_certificate_type.name name
  from dbo.d_dim_spabiz_gift_certificate_type;