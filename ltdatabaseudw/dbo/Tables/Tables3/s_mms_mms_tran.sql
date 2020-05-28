CREATE TABLE [dbo].[s_mms_mms_tran] (
    [s_mms_mms_tran_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [mms_tran_id]                INT             NULL,
    [domain_name]                VARCHAR (50)    NULL,
    [receipt_number]             VARCHAR (50)    NULL,
    [receipt_comment]            VARCHAR (255)   NULL,
    [post_date_time]             DATETIME        NULL,
    [tran_date]                  DATETIME        NULL,
    [pos_amount]                 DECIMAL (26, 6) NULL,
    [tran_amount]                DECIMAL (26, 6) NULL,
    [change_rendered]            DECIMAL (26, 6) NULL,
    [utc_post_date_time]         DATETIME        NULL,
    [post_date_time_zone]        VARCHAR (4)     NULL,
    [inserted_date_time]         DATETIME        NULL,
    [updated_date_time]          DATETIME        NULL,
    [tran_edited_flag]           BIT             NULL,
    [tran_edited_date_time]      DATETIME        NULL,
    [utc_tran_edited_date_time]  DATETIME        NULL,
    [tran_edited_date_time_zone] VARCHAR (4)     NULL,
    [reverse_tran_flag]          BIT             NULL,
    [computer_name]              VARCHAR (15)    NULL,
    [ip_address]                 VARCHAR (16)    NULL,
    [converted_amount]           DECIMAL (26, 6) NULL,
    [refunded_as_product_flag]   BIT             NULL,
    [transaction_source]         VARCHAR (50)    NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_r_load_source_id]        BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_hash]                    CHAR (32)       NOT NULL,
    [dv_deleted]                 BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_mms_tran]([dv_batch_id] ASC);

