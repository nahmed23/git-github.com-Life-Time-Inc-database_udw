CREATE TABLE [dbo].[stage_hash_etips_opt_in_employees] (
    [stage_hash_etips_opt_in_employees_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [employee_id]                          VARCHAR (255) NULL,
    [pay_card_start_date]                  VARCHAR (255) NULL,
    [pay_card_status]                      VARCHAR (255) NULL,
    [ltf_file_name]                        VARCHAR (255) NULL,
    [dummy_modified_date_time]             DATETIME      NULL,
    [dv_load_date_time]                    DATETIME      NOT NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

