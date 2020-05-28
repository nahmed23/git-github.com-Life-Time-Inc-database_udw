CREATE VIEW [sandbox].[v_mart_mms_val_termination_reason_termed_with_balance_exclude]
AS SELECT [val_termination_reason_id]
     , [description]
     , [sort_order]
     , [inserted_date_time]
     , [updated_date_time]
     , [display_ui_flag]
     , [additional_reason_display_ui_flag] = 0
     , [dim_mms_val_termination_reason_key] = [bk_hash]
     , [bk_hash]
     , [r_mms_val_termination_reason_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash]
     , [termination_reason_description] = [description]
  FROM [dbo].[r_mms_val_termination_reason]
  WHERE [dv_load_end_date_time] = '9999-12-31 00:00:00.000'
    AND [val_termination_reason_id] IN (21,26,27,28,32,34,35,41,42,47,49,50,58,59,65,73,159,161,162,164);