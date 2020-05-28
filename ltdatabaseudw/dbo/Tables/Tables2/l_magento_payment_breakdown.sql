CREATE TABLE [dbo].[l_magento_payment_breakdown] (
    [l_magento_payment_breakdown_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [OrderNum]                       VARCHAR (255) NULL,
    [OENum]                          INT           NULL,
    [TranDate]                       VARCHAR (29)  NULL,
    [club_id]                        INT           NULL,
    [offering_id]                    VARCHAR (255) NULL,
    [product_id]                     VARCHAR (255) NULL,
    [mms_product_id]                 INT           NULL,
    [mms_transaction_id]             VARCHAR (255) NULL,
    [mms_package_id]                 INT           NULL,
    [member_id]                      VARCHAR (255) NULL,
    [dv_load_date_time]              DATETIME      NOT NULL,
    [dv_r_load_source_id]            BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL,
    [dv_hash]                        CHAR (32)     NOT NULL,
    [dv_deleted]                     BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

