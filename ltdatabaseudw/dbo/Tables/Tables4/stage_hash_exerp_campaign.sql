﻿CREATE TABLE [dbo].[stage_hash_exerp_campaign] (
    [stage_hash_exerp_campaign_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [id]                           VARCHAR (4000) NULL,
    [state]                        VARCHAR (4000) NULL,
    [type]                         VARCHAR (4000) NULL,
    [name]                         VARCHAR (4000) NULL,
    [start_date]                   DATETIME       NULL,
    [end_date]                     DATETIME       NULL,
    [campaign_codes_type]          VARCHAR (4000) NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

