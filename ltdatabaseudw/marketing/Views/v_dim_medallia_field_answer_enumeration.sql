CREATE VIEW [marketing].[v_dim_medallia_field_answer_enumeration]
AS select d_medallia_field_answer_enumeration.dim_medallia_field_answer_enumeration_key dim_medallia_field_answer_enumeration_key,
       d_medallia_field_answer_enumeration.answer_enumeration_id answer_enumeration_id,
       d_medallia_field_answer_enumeration.answer_id answer_id,
       d_medallia_field_answer_enumeration.answer_name answer_name,
       d_medallia_field_answer_enumeration.dim_medallia_field_answer_key dim_medallia_field_answer_key,
       d_medallia_field_answer_enumeration.enumeration_value enumeration_value
  from dbo.d_medallia_field_answer_enumeration;