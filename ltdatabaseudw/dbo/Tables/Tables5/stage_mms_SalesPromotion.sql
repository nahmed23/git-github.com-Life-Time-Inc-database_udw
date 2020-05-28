﻿CREATE TABLE [dbo].[stage_mms_SalesPromotion] (
    [stage_mms_SalesPromotion_id]       BIGINT        NOT NULL,
    [SalesPromotionID]                  INT           NULL,
    [EffectiveFromDateTime]             DATETIME      NULL,
    [EffectiveThruDateTime]             DATETIME      NULL,
    [DisplayText]                       VARCHAR (255) NULL,
    [ReceiptText]                       VARCHAR (50)  NULL,
    [ValSalesPromotionTypeID]           INT           NULL,
    [AvailableForAllSalesChannelsFlag]  BIT           NULL,
    [AvailableForAllClubsFlag]          BIT           NULL,
    [AvailableForAllCustomersFlag]      BIT           NULL,
    [InsertedDateTime]                  DATETIME      NULL,
    [UpdatedDateTime]                   DATETIME      NULL,
    [PromotionOwnerEmployeeID]          INT           NULL,
    [PromotionCodeUsageLimit]           INT           NULL,
    [PromotionCodeRequiredFlag]         BIT           NULL,
    [PromotionCodeIssuerCreateLimit]    INT           NULL,
    [PromotionCodeOverallCreateLimit]   INT           NULL,
    [CompanyID]                         INT           NULL,
    [ExcludeMyHealthCheckFlag]          BIT           NULL,
    [ValRevenueReportingCategoryID]     INT           NULL,
    [ValSalesReportingCategoryID]       INT           NULL,
    [ExcludeFromAttritionReportingFlag] BIT           NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

