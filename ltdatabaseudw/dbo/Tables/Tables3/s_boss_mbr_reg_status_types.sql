﻿CREATE TABLE [dbo].[s_boss_mbr_reg_status_types] (
    [s_boss_mbr_reg_status_types_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [mbr_reg_status_types_id]        INT           NULL,
    [name]                           VARCHAR (50)  NULL,
    [description]                    VARCHAR (100) NULL,
    [position]                       INT           NULL,
    [term_type]                      CHAR (1)      NULL,
    [active]                         CHAR (1)      NULL,
    [created_at]                     DATETIME      NULL,
    [updated_at]                     DATETIME      NULL,
    [dv_load_date_time]              DATETIME      NOT NULL,
    [dv_r_load_source_id]            BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL,
    [dv_hash]                        CHAR (32)     NOT NULL,
    [dv_deleted]                     BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_mbr_reg_status_types]([dv_batch_id] ASC);

