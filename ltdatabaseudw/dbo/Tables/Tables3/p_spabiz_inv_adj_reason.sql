﻿CREATE TABLE [dbo].[p_spabiz_inv_adj_reason] (
    [p_spabiz_inv_adj_reason_id]           BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [inv_adj_reason_id]                    DECIMAL (26, 6) NULL,
    [store_number]                         DECIMAL (26, 6) NULL,
    [l_spabiz_inv_adj_reason_id]           BIGINT          NULL,
    [s_spabiz_inv_adj_reason_id]           BIGINT          NULL,
    [dv_greatest_satellite_date_time]      DATETIME        NULL,
    [dv_next_greatest_satellite_date_time] DATETIME        NULL,
    [dv_load_date_time]                    DATETIME        NOT NULL,
    [dv_load_end_date_time]                DATETIME        NOT NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL,
    [dv_first_in_key_series]               BIT             NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_spabiz_inv_adj_reason]
    ON [dbo].[p_spabiz_inv_adj_reason]([bk_hash] ASC, [p_spabiz_inv_adj_reason_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_spabiz_inv_adj_reason]([dv_batch_id] ASC);

