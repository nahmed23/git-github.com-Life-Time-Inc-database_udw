CREATE TABLE [dbo].[stage_boss_attendance] (
    [stage_boss_attendance_id] BIGINT       NOT NULL,
    [reservation]              INT          NULL,
    [attendance_date]          DATETIME     NULL,
    [cust_code]                CHAR (10)    NULL,
    [mbr_code]                 CHAR (10)    NULL,
    [checked_in]               CHAR (1)     NULL,
    [employee_id]              CHAR (10)    NULL,
    [comment]                  VARCHAR (80) NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

