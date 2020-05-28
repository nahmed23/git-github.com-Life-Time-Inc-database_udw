﻿CREATE VIEW [sandbox].[v_mart_mms_val_membership_upgrade_date_range]
AS SELECT [val_membership_upgrade_date_range_id]
     , [description]
     , [sort_order]
     , [inserted_date_time]
     , [updated_date_time]
     , [dim_mms_val_membership_upgrade_date_range_key] = [bk_hash]
     , [bk_hash]
     , [r_mms_val_membership_upgrade_date_range_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash]
  FROM [dbo].[r_mms_val_membership_upgrade_date_range]
  WHERE [dv_load_end_date_time] = '9999-12-31 00:00:00.000';