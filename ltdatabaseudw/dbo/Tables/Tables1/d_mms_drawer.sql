CREATE TABLE [dbo].[d_mms_drawer] (
    [d_mms_drawer_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [dim_mms_drawer_key]    CHAR (32)       NULL,
    [drawer_id]             INT             NULL,
    [club_id]               INT             NULL,
    [description]           VARCHAR (50)    NULL,
    [dim_club_key]          CHAR (32)       NULL,
    [locked_flag]           CHAR (1)        NULL,
    [starting_cash_amount]  DECIMAL (26, 6) NULL,
    [p_mms_drawer_id]       BIGINT          NOT NULL,
    [dv_load_date_time]     DATETIME        NULL,
    [dv_load_end_date_time] DATETIME        NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_drawer]([dv_batch_id] ASC);

