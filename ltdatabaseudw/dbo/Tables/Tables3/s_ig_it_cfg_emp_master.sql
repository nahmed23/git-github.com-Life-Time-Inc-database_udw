CREATE TABLE [dbo].[s_ig_it_cfg_emp_master] (
    [s_ig_it_cfg_emp_master_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)     NOT NULL,
    [emp_id]                    INT           NULL,
    [emp_pos_name]              NVARCHAR (16) NULL,
    [emp_first_name]            NVARCHAR (16) NULL,
    [emp_last_name]             NVARCHAR (50) NULL,
    [emp_hire_dt]               DATETIME      NULL,
    [emp_terminate_dt]          DATETIME      NULL,
    [emp_card_no]               INT           NULL,
    [dummy_modified_date_time]  DATETIME      NULL,
    [dv_load_date_time]         DATETIME      NOT NULL,
    [dv_r_load_source_id]       BIGINT        NOT NULL,
    [dv_inserted_date_time]     DATETIME      NOT NULL,
    [dv_insert_user]            VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]      DATETIME      NULL,
    [dv_update_user]            VARCHAR (50)  NULL,
    [dv_hash]                   CHAR (32)     NOT NULL,
    [dv_deleted]                BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

