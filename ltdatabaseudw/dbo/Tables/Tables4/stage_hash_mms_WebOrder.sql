CREATE TABLE [dbo].[stage_hash_mms_WebOrder] (
    [stage_hash_mms_WebOrder_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [WebOrderID]                 INT             NULL,
    [PartyEncryptionID]          INT             NULL,
    [ValProductSalesChannelID]   INT             NULL,
    [PlacedOrderTotal]           DECIMAL (26, 6) NULL,
    [RevisedOrderTotal]          DECIMAL (26, 6) NULL,
    [BalanceDue]                 DECIMAL (26, 6) NULL,
    [PlacedDateTime]             DATETIME        NULL,
    [RevisedDateTime]            DATETIME        NULL,
    [IPAddress]                  VARCHAR (16)    NULL,
    [ExpirationDateTime]         DATETIME        NULL,
    [ValWebOrderStatusID]        INT             NULL,
    [InsertedDateTime]           DATETIME        NULL,
    [UpdatedDateTime]            DATETIME        NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

