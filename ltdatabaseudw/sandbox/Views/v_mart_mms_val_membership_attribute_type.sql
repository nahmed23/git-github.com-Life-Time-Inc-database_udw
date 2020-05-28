﻿CREATE VIEW [sandbox].[v_mart_mms_val_membership_attribute_type]
AS SELECT [val_membership_attribute_type_id]
     , [description]
     , [display_ui_flag]
     , [sort_order]
     , [inserted_date_time]
     , [updated_date_time]
     , [dim_mms_val_membership_attribute_type_key] = [bk_hash]
     , [bk_hash]
     , [r_mms_val_membership_attribute_type_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash]
  FROM [dbo].[r_mms_val_membership_attribute_type]
  WHERE [dv_load_date_time] = '9999-12-31 00:00:00.000';