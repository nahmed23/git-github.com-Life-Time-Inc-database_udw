﻿CREATE TABLE [dbo].[stage_hash_exerp_freeze_period] (
    [stage_hash_exerp_freeze_period_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [id]                                INT            NULL,
    [subscription_id]                   VARCHAR (4000) NULL,
    [subscription_center_id]            VARCHAR (4000) NULL,
    [start_date]                        DATETIME       NULL,
    [end_date]                          DATETIME       NULL,
    [state]                             VARCHAR (4000) NULL,
    [type]                              VARCHAR (4000) NULL,
    [reason]                            VARCHAR (4000) NULL,
    [entry_datetime]                    DATETIME       NULL,
    [cancel_datetime]                   DATETIME       NULL,
    [center_id]                         INT            NULL,
    [ets]                               BIGINT         NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

