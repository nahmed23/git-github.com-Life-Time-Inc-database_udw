CREATE VIEW [sandbox].[v_mart_mms_department]
AS SELECT [department_id]
     , [description]
     , [name]
     , [sort_order]
     , [dim_mms_department_key]
     , [bk_hash]
     , [p_mms_department_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [department_description] = [description]
     , [distribution_list]      = CASE WHEN [department_id] IN (21) THEN 'AquaticsManager@lt.life'
                                       WHEN [department_id] IN (16,24,25,26,29,30,32) THEN 'KidsActivitiesManager@lt.life'
                                       WHEN [department_id] IN (1,2,11,18,31,35,36) THEN 'MemberServicesManager@lt.life'
                                       WHEN [department_id] IN (3,6,9,10,19,33,37) THEN 'PTManager@lt.life'
                                       WHEN [department_id] IN (8,12) THEN 'SpaManager@lt.life'
                                       WHEN [department_id] IN (20,27,34) THEN 'StudioManager@lt.life'
                                       WHEN [department_id] IN (15,17) THEN 'TennisManager@lt.life'
                                       ELSE 'GeneralManager@lt.life' END  --4,5,7,13,14,22,23,28
  FROM [dbo].[d_mms_department] DIM;