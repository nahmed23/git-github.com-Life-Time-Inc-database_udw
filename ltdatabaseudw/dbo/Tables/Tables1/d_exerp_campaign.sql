﻿CREATE TABLE [dbo].[d_exerp_campaign] (
    [d_exerp_campaign_id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [campaign_id]           VARCHAR (4000) NULL,
    [campaign_codes_type]   VARCHAR (4000) NULL,
    [campaign_name]         VARCHAR (4000) NULL,
    [campaign_state]        VARCHAR (4000) NULL,
    [campaign_type]         VARCHAR (4000) NULL,
    [end_dim_date_key]      CHAR (8)       NULL,
    [start_dim_date_key]    CHAR (8)       NULL,
    [p_exerp_campaign_id]   BIGINT         NOT NULL,
    [deleted_flag]          INT            NULL,
    [dv_load_date_time]     DATETIME       NULL,
    [dv_load_end_date_time] DATETIME       NULL,
    [dv_batch_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_campaign]([dv_batch_id] ASC);

