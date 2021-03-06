﻿CREATE TABLE [dbo].[stage_hash_mms_tranitem] (
    [stage_hash_mms_tranitem_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [TranItemID]                 INT             NULL,
    [MMSTranID]                  INT             NULL,
    [ProductID]                  INT             NULL,
    [Quantity]                   INT             NULL,
    [ItemSalesTax]               DECIMAL (26, 6) NULL,
    [ItemAmount]                 DECIMAL (26, 6) NULL,
    [InsertedDateTime]           DATETIME        NULL,
    [SoldNotServicedFlag]        BIT             NULL,
    [UpdatedDateTime]            DATETIME        NULL,
    [ItemDiscountAmount]         DECIMAL (26, 6) NULL,
    [ClubID]                     INT             NULL,
    [BundleProductID]            INT             NULL,
    [ExternalItemID]             VARCHAR (50)    NULL,
    [ItemLTBucksAmount]          DECIMAL (26, 6) NULL,
    [TransactionSource]          VARCHAR (50)    NULL,
    [ItemLTBucksSalesTax]        DECIMAL (26, 6) NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

