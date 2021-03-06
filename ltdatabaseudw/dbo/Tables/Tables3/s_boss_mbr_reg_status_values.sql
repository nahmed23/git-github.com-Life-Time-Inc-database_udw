﻿CREATE TABLE [dbo].[s_boss_mbr_reg_status_values] (
    [s_boss_mbr_reg_status_values_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [mbr_reg_status_values_id]        INT           NULL,
    [cust_code]                       VARCHAR (10)  NULL,
    [mbr_code]                        VARCHAR (10)  NULL,
    [start_date]                      DATETIME      NULL,
    [end_date]                        DATETIME      NULL,
    [reg_status_type_id]              INT           NULL,
    [created_at]                      DATETIME      NULL,
    [updated_at]                      DATETIME      NULL,
    [notes]                           VARCHAR (800) NULL,
    [value]                           VARCHAR (100) NULL,
    [dv_load_date_time]               DATETIME      NOT NULL,
    [dv_r_load_source_id]             BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL,
    [dv_hash]                         CHAR (32)     NOT NULL,
    [dv_deleted]                      BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_mbr_reg_status_values]([dv_batch_id] ASC);

