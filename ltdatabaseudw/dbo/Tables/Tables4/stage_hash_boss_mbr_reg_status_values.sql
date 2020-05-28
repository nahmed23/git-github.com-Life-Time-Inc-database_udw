CREATE TABLE [dbo].[stage_hash_boss_mbr_reg_status_values] (
    [stage_hash_boss_mbr_reg_status_values_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)     NOT NULL,
    [id]                                       INT           NULL,
    [cust_code]                                VARCHAR (10)  NULL,
    [mbr_code]                                 VARCHAR (10)  NULL,
    [start_date]                               DATETIME      NULL,
    [end_date]                                 DATETIME      NULL,
    [reg_status_type_id]                       INT           NULL,
    [created_at]                               DATETIME      NULL,
    [updated_at]                               DATETIME      NULL,
    [notes]                                    VARCHAR (800) NULL,
    [value]                                    VARCHAR (100) NULL,
    [dv_load_date_time]                        DATETIME      NOT NULL,
    [dv_inserted_date_time]                    DATETIME      NOT NULL,
    [dv_insert_user]                           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                     DATETIME      NULL,
    [dv_update_user]                           VARCHAR (50)  NULL,
    [dv_batch_id]                              BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

