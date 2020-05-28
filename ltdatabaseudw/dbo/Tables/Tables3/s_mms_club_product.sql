CREATE TABLE [dbo].[s_mms_club_product] (
    [s_mms_club_product_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [club_product_id]       INT             NULL,
    [price]                 DECIMAL (26, 6) NULL,
    [inserted_date_time]    DATETIME        NULL,
    [sold_in_pk]            BIT             NULL,
    [updated_date_time]     DATETIME        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_club_product]
    ON [dbo].[s_mms_club_product]([bk_hash] ASC, [s_mms_club_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_club_product]([dv_batch_id] ASC);

