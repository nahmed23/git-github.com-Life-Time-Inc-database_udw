﻿CREATE TABLE [dbo].[h_spabiz_ticket_discount] (
    [h_spabiz_ticket_discount_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [ticket_discount_id]          DECIMAL (26, 6) NULL,
    [store_number]                DECIMAL (26, 6) NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_deleted]                  BIT             DEFAULT ((0)) NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_spabiz_ticket_discount]
    ON [dbo].[h_spabiz_ticket_discount]([bk_hash] ASC, [h_spabiz_ticket_discount_id] ASC);

