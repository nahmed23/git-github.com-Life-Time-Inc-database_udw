CREATE TABLE [dbo].[d_spabiz_gift] (
    [d_spabiz_gift_id]                     BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [dim_spabiz_gift_certificate_type_key] CHAR (32)     NULL,
    [gift_id]                              BIGINT        NULL,
    [store_number]                         BIGINT        NULL,
    [deleted_date_time]                    DATETIME      NULL,
    [deleted_flag]                         CHAR (1)      NULL,
    [dim_spabiz_store_key]                 CHAR (32)     NULL,
    [edit_date_time]                       DATETIME      NULL,
    [name]                                 VARCHAR (150) NULL,
    [p_spabiz_gift_id]                     BIGINT        NOT NULL,
    [dv_load_date_time]                    DATETIME      NULL,
    [dv_load_end_date_time]                DATETIME      NULL,
    [dv_batch_id]                          BIGINT        NOT NULL,
    [dv_inserted_date_time]                DATETIME      NOT NULL,
    [dv_insert_user]                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                 DATETIME      NULL,
    [dv_update_user]                       VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_gift]([dv_batch_id] ASC);

