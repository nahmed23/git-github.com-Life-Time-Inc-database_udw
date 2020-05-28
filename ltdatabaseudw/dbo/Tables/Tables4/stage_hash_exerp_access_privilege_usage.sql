﻿CREATE TABLE [dbo].[stage_hash_exerp_access_privilege_usage] (
    [stage_hash_exerp_access_privilege_usage_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [id]                                         INT            NULL,
    [source_type]                                VARCHAR (4000) NULL,
    [source_id]                                  VARCHAR (4000) NULL,
    [target_type]                                VARCHAR (4000) NULL,
    [target_id]                                  VARCHAR (4000) NULL,
    [state]                                      VARCHAR (4000) NULL,
    [deduction_key]                              VARCHAR (4000) NULL,
    [punishment_key]                             VARCHAR (4000) NULL,
    [center_id]                                  INT            NULL,
    [ets]                                        BIGINT         NULL,
    [dummy_modified_date_time]                   DATETIME       NULL,
    [dv_load_date_time]                          DATETIME       NOT NULL,
    [dv_batch_id]                                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

