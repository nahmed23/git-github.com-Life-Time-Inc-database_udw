﻿CREATE TABLE [dbo].[s_mms_email_address_status] (
    [s_mms_email_address_status_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [email_address_status_id]       INT           NULL,
    [email_address]                 VARCHAR (140) NULL,
    [status_from_date]              DATETIME      NULL,
    [status_thru_date]              DATETIME      NULL,
    [inserted_date_time]            DATETIME      NULL,
    [updated_date_time]             DATETIME      NULL,
    [email_address_search]          VARCHAR (10)  NULL,
    [dv_load_date_time]             DATETIME      NOT NULL,
    [dv_batch_id]                   BIGINT        NOT NULL,
    [dv_r_load_source_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL,
    [dv_hash]                       CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_email_address_status]
    ON [dbo].[s_mms_email_address_status]([bk_hash] ASC, [s_mms_email_address_status_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_email_address_status]([dv_batch_id] ASC);

