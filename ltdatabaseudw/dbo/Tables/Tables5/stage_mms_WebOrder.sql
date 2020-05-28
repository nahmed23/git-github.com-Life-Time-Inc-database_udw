CREATE TABLE [dbo].[stage_mms_WebOrder] (
    [stage_mms_WebOrder_id]    BIGINT          NOT NULL,
    [WebOrderID]               INT             NULL,
    [PartyEncryptionID]        INT             NULL,
    [ValProductSalesChannelID] INT             NULL,
    [PlacedOrderTotal]         DECIMAL (26, 6) NULL,
    [RevisedOrderTotal]        DECIMAL (26, 6) NULL,
    [BalanceDue]               DECIMAL (26, 6) NULL,
    [PlacedDateTime]           DATETIME        NULL,
    [RevisedDateTime]          DATETIME        NULL,
    [IPAddress]                VARCHAR (16)    NULL,
    [ExpirationDateTime]       DATETIME        NULL,
    [ValWebOrderStatusID]      INT             NULL,
    [InsertedDateTime]         DATETIME        NULL,
    [UpdatedDateTime]          DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

