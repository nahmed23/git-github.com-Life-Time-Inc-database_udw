CREATE TABLE [dbo].[stage_hash_ig_it_cfg_Emp_Master] (
    [stage_hash_ig_it_cfg_Emp_Master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)     NOT NULL,
    [emp_id]                             INT           NULL,
    [emp_pos_name]                       NVARCHAR (16) NULL,
    [emp_first_name]                     NVARCHAR (16) NULL,
    [emp_last_name]                      NVARCHAR (50) NULL,
    [supervisor_emp_id]                  INT           NULL,
    [emp_hire_dt]                        DATETIME      NULL,
    [emp_terminate_dt]                   DATETIME      NULL,
    [emp_card_no]                        INT           NULL,
    [store_id]                           INT           NULL,
    [dummy_modified_date_time]           DATETIME      NULL,
    [dv_load_date_time]                  DATETIME      NOT NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

