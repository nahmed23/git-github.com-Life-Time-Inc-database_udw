﻿CREATE TABLE [dbo].[s_mms_payment] (
    [s_mms_payment_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [payment_id]            INT             NULL,
    [payment_amount]        DECIMAL (26, 6) NULL,
    [approval_code]         VARCHAR (50)    NULL,
    [inserted_date_time]    DATETIME        NULL,
    [updated_date_time]     DATETIME        NULL,
    [tip_amount]            DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_payment]
    ON [dbo].[s_mms_payment]([bk_hash] ASC, [s_mms_payment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_payment]([dv_batch_id] ASC);

