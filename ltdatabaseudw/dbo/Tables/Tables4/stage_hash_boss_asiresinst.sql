﻿CREATE TABLE [dbo].[stage_hash_boss_asiresinst] (
    [stage_hash_boss_asiresinst_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [reservation]                   INT             NULL,
    [instructor_id]                 INT             NULL,
    [start_date]                    DATETIME        NULL,
    [end_date]                      DATETIME        NULL,
    [name]                          CHAR (30)       NULL,
    [comment]                       VARCHAR (80)    NULL,
    [cost]                          DECIMAL (26, 6) NULL,
    [substitute]                    CHAR (1)        NULL,
    [sub_for]                       CHAR (10)       NULL,
    [id]                            INT             NULL,
    [employee_id]                   INT             NULL,
    [updated_at]                    DATETIME        NULL,
    [created_at]                    DATETIME        NULL,
    [res_color]                     INT             NULL,
    [use_for_LTBucks]               CHAR (1)        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

