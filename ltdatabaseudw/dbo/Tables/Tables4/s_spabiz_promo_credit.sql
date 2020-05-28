﻿CREATE TABLE [dbo].[s_spabiz_promo_credit] (
    [s_spabiz_promo_credit_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [promo_credit_id]          DECIMAL (26, 6) NULL,
    [counter_id]               DECIMAL (26, 6) NULL,
    [edit_time]                DATETIME        NULL,
    [status]                   DECIMAL (26, 6) NULL,
    [serial_num]               VARCHAR (150)   NULL,
    [amount]                   DECIMAL (26, 6) NULL,
    [balance]                  DECIMAL (26, 6) NULL,
    [date]                     DATETIME        NULL,
    [days_good]                DECIMAL (26, 6) NULL,
    [last_used]                DATETIME        NULL,
    [address_1]                VARCHAR (150)   NULL,
    [store_number]             DECIMAL (26, 6) NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_batch_id]              BIGINT          NOT NULL,
    [dv_r_load_source_id]      BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_hash]                  CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_promo_credit]
    ON [dbo].[s_spabiz_promo_credit]([bk_hash] ASC, [s_spabiz_promo_credit_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_promo_credit]([dv_batch_id] ASC);

