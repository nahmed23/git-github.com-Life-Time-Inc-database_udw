CREATE VIEW [marketing].[v_dim_medallia_field_answer]
AS select d_medallia_field_answer.answer_id answer_id,
       d_medallia_field_answer.answer_name answer_name,
       d_medallia_field_answer.answer_type answer_type,
       d_medallia_field_answer.dim_medallia_field_answer_key dim_medallia_field_answer_key
  from dbo.d_medallia_field_answer;