CREATE VIEW [marketing].[v_dim_medallia_field]
AS select d_medallia_field.name_in_medallia name_in_medallia,
       d_medallia_field.answer_id answer_id,
       d_medallia_field.data_type data_type,
       d_medallia_field.description_question description_question,
       d_medallia_field.dim_medallia_field_answer_key dim_medallia_field_answer_key,
       d_medallia_field.examples examples,
       d_medallia_field.name_in_api name_in_api,
       d_medallia_field.single_select single_select,
       d_medallia_field.sr_no sr_no,
       d_medallia_field.variable_name variable_name
  from dbo.d_medallia_field;