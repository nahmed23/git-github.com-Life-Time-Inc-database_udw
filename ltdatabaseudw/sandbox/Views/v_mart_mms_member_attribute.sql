CREATE VIEW [sandbox].[v_mart_mms_member_attribute]
AS SELECT DIM.[member_attribute_id]
     , DIM.[member_id]
     , DIM.[val_member_attribute_type_id]
     , DIM.[attribute_value]
     , [effective_from_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_from_date_time]), 0)
     , DIM.[effective_from_date_time]
     , [effective_thru_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_thru_date_time]), 0)
     , DIM.[effective_thru_date_time]
     , DIM.[expiration_date]
     , DIM.[inserted_date_time]
     , DIM.[updated_date_time]
     , DIM.[dim_mms_member_key]
     , DIM.[bk_hash]
     , DIM.[p_mms_member_attribute_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , DIM.[deleted_flag]
     , [val_card_level_id] = case when DIM.[val_member_attribute_type_id]=(4) AND isnumeric(DIM.[attribute_value])=(1) AND CONVERT([decimal],DIM.[attribute_value],(0))<=(2147483647) then CONVERT([int],DIM.[attribute_value],(0)) end
  FROM [dbo].[d_mms_member_attribute] DIM;