CREATE VIEW [marketing].[v_dim_description]
AS select dim_description.dim_description_key dim_description_key,
       dim_description.source_object source_object,
       dim_description.source_bk_hash source_bk_hash,
       dim_description.abbreviated_description abbreviated_description,
       dim_description.description description
  from dbo.dim_description;