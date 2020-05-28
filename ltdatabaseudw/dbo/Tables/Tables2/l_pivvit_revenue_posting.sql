CREATE TABLE [dbo].[l_pivvit_revenue_posting] (
    [l_pivvit_revenue_posting_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [transaction_id]              NVARCHAR (500) NULL,
    [pivvit_order_number]         VARCHAR (10)   NULL,
    [pivvit_line_number]          VARCHAR (10)   NULL,
    [company_id]                  VARCHAR (50)   NULL,
    [cost_center_id]              VARCHAR (100)  NULL,
    [currency_id]                 VARCHAR (50)   NULL,
    [club_id]                     INT            NULL,
    [tender_type_id]              NVARCHAR (50)  NULL,
    [mms_product_code]            INT            NULL,
    [dv_load_date_time]           DATETIME       NOT NULL,
    [dv_r_load_source_id]         BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL,
    [dv_hash]                     CHAR (32)      NOT NULL,
    [dv_deleted]                  BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

