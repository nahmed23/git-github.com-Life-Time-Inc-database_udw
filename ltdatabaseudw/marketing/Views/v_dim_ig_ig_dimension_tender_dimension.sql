CREATE VIEW [marketing].[v_dim_ig_ig_dimension_tender_dimension]
AS select d_ig_ig_dimension_tender_dimension.tender_dim_id tender_dim_id,
       d_ig_ig_dimension_tender_dimension.additional_check_id_code_id additional_check_id_code_id,
       d_ig_ig_dimension_tender_dimension.cash_tender_flag cash_tender_flag,
       d_ig_ig_dimension_tender_dimension.comp_tender_flag comp_tender_flag,
       d_ig_ig_dimension_tender_dimension.corp_id corp_id,
       d_ig_ig_dimension_tender_dimension.customer_id customer_id,
       d_ig_ig_dimension_tender_dimension.dim_cafe_payment_type_key dim_cafe_payment_type_key,
       d_ig_ig_dimension_tender_dimension.eff_date_from eff_date_from,
       d_ig_ig_dimension_tender_dimension.eff_date_to eff_date_to,
       d_ig_ig_dimension_tender_dimension.effective_dim_date_key effective_dim_date_key,
       d_ig_ig_dimension_tender_dimension.ent_id ent_id,
       d_ig_ig_dimension_tender_dimension.expiration_dim_date_key expiration_dim_date_key,
       d_ig_ig_dimension_tender_dimension.payment_class payment_class,
       d_ig_ig_dimension_tender_dimension.payment_id payment_id,
       d_ig_ig_dimension_tender_dimension.payment_type payment_type,
       d_ig_ig_dimension_tender_dimension.profit_center_dim_level_2_id profit_center_dim_level_2_id,
       d_ig_ig_dimension_tender_dimension.tender_id tender_id
  from dbo.d_ig_ig_dimension_tender_dimension;