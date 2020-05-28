﻿CREATE TABLE [dbo].[s_sandbox_service_mapping] (
    [s_sandbox_service_mapping_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [service_id]                   BIGINT        NULL,
    [store_number]                 INT           NULL,
    [service_name]                 VARCHAR (100) NULL,
    [category]                     VARCHAR (25)  NULL,
    [segment]                      VARCHAR (25)  NULL,
    [updated_date_time]            DATETIME      NULL,
    [commission_mapping]           VARCHAR (25)  NULL,
    [jan_one]                      DATETIME      NULL,
    [dv_load_date_time]            DATETIME      NOT NULL,
    [dv_r_load_source_id]          BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL,
    [dv_hash]                      CHAR (32)     NOT NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_sandbox_service_mapping]
    ON [dbo].[s_sandbox_service_mapping]([bk_hash] ASC, [s_sandbox_service_mapping_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_sandbox_service_mapping]([dv_batch_id] ASC);

