CREATE TABLE [dbo].[stage_etips_opt_in_employees] (
    [stage_etips_opt_in_employees_id] BIGINT        NOT NULL,
    [employee_id]                     VARCHAR (255) NULL,
    [pay_card_start_date]             VARCHAR (255) NULL,
    [pay_card_status]                 VARCHAR (255) NULL,
    [ltf_file_name]                   VARCHAR (255) NULL,
    [dummy_modified_date_time]        DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

