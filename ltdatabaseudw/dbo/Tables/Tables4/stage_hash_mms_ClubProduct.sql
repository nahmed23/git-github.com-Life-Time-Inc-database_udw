CREATE TABLE [dbo].[stage_hash_mms_ClubProduct] (
    [stage_hash_mms_ClubProduct_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [ClubProductID]                 INT             NULL,
    [ClubID]                        INT             NULL,
    [ProductID]                     INT             NULL,
    [Price]                         DECIMAL (26, 6) NULL,
    [ValCommissionableID]           TINYINT         NULL,
    [InsertedDateTime]              DATETIME        NULL,
    [SoldInPK]                      BIT             NULL,
    [UpdatedDateTime]               DATETIME        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

