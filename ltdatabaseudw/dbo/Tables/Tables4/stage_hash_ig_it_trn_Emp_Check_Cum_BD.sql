CREATE TABLE [dbo].[stage_hash_ig_it_trn_Emp_Check_Cum_BD] (
    [stage_hash_ig_it_trn_Emp_Check_Cum_BD_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32) NOT NULL,
    [bus_day_id]                               INT       NULL,
    [check_type_id]                            INT       NULL,
    [meal_period_id]                           INT       NULL,
    [num_checks]                               INT       NULL,
    [num_covers]                               INT       NULL,
    [profit_center_id]                         INT       NULL,
    [server_emp_id]                            INT       NULL,
    [void_type_id]                             INT       NULL,
    [dummy_modified_date_time]                 DATETIME  NULL,
    [dv_load_date_time]                        DATETIME  NOT NULL,
    [dv_batch_id]                              BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

