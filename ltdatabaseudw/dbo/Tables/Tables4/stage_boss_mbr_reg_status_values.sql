CREATE TABLE [dbo].[stage_boss_mbr_reg_status_values] (
    [stage_boss_mbr_reg_status_values_id] BIGINT        NOT NULL,
    [id]                                  INT           NULL,
    [cust_code]                           VARCHAR (10)  NULL,
    [mbr_code]                            VARCHAR (10)  NULL,
    [start_date]                          DATETIME      NULL,
    [end_date]                            DATETIME      NULL,
    [reg_status_type_id]                  INT           NULL,
    [created_at]                          DATETIME      NULL,
    [updated_at]                          DATETIME      NULL,
    [notes]                               VARCHAR (800) NULL,
    [value]                               VARCHAR (100) NULL,
    [dv_batch_id]                         BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

