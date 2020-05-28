﻿CREATE TABLE [dbo].[l_hybris_payment_transactions] (
    [l_hybris_payment_transactions_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)      NOT NULL,
    [type_pk_string]                   BIGINT         NULL,
    [payment_transactions_pk]          BIGINT         NULL,
    [owner_pk_string]                  BIGINT         NULL,
    [p_version_id]                     NVARCHAR (255) NULL,
    [p_code]                           NVARCHAR (255) NULL,
    [p_request_id]                     NVARCHAR (255) NULL,
    [dv_load_date_time]                DATETIME       NOT NULL,
    [dv_r_load_source_id]              BIGINT         NOT NULL,
    [dv_inserted_date_time]            DATETIME       NOT NULL,
    [dv_insert_user]                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]             DATETIME       NULL,
    [dv_update_user]                   VARCHAR (50)   NULL,
    [dv_hash]                          CHAR (32)      NOT NULL,
    [dv_batch_id]                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_payment_transactions]
    ON [dbo].[l_hybris_payment_transactions]([bk_hash] ASC, [l_hybris_payment_transactions_id] ASC);

