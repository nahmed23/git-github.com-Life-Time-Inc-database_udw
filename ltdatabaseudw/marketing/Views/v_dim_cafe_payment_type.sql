CREATE VIEW [marketing].[v_dim_cafe_payment_type] AS select dim_cafe_payment_type.dim_cafe_payment_type_key dim_cafe_payment_type_key,
       dim_cafe_payment_type.payment_class payment_class,
       dim_cafe_payment_type.payment_type payment_type,
       dim_cafe_payment_type.tender_id tender_id
  from dbo.dim_cafe_payment_type;