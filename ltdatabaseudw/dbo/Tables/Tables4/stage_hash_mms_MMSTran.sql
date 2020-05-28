﻿CREATE TABLE [dbo].[stage_hash_mms_MMSTran] (
    [stage_hash_mms_MMSTran_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [MMSTranID]                  INT             NULL,
    [ClubID]                     INT             NULL,
    [MembershipID]               INT             NULL,
    [MemberID]                   INT             NULL,
    [DrawerActivityID]           INT             NULL,
    [TranVoidedID]               INT             NULL,
    [ReasonCodeID]               INT             NULL,
    [ValTranTypeID]              TINYINT         NULL,
    [DomainName]                 VARCHAR (50)    NULL,
    [ReceiptNumber]              VARCHAR (50)    NULL,
    [ReceiptComment]             VARCHAR (255)   NULL,
    [PostDateTime]               DATETIME        NULL,
    [EmployeeID]                 INT             NULL,
    [TranDate]                   DATETIME        NULL,
    [POSAmount]                  DECIMAL (26, 6) NULL,
    [TranAmount]                 DECIMAL (26, 6) NULL,
    [OriginalDrawerActivityID]   INT             NULL,
    [ChangeRendered]             DECIMAL (26, 6) NULL,
    [UTCPostDateTime]            DATETIME        NULL,
    [PostDateTimeZone]           VARCHAR (4)     NULL,
    [InsertedDateTime]           DATETIME        NULL,
    [UpdatedDateTime]            DATETIME        NULL,
    [OriginalMMSTranID]          INT             NULL,
    [TranEditedFlag]             BIT             NULL,
    [TranEditedEmployeeID]       INT             NULL,
    [TranEditedDateTime]         DATETIME        NULL,
    [UTCTranEditedDateTime]      DATETIME        NULL,
    [TranEditedDateTimeZone]     VARCHAR (4)     NULL,
    [ReverseTranFlag]            BIT             NULL,
    [ComputerName]               VARCHAR (15)    NULL,
    [IPAddress]                  VARCHAR (16)    NULL,
    [ValCurrencyCodeID]          TINYINT         NULL,
    [CorporatePartnerID]         INT             NULL,
    [ConvertedAmount]            DECIMAL (26, 6) NULL,
    [ConvertedValCurrencyCodeID] TINYINT         NULL,
    [ReimbursementProgramID]     INT             NULL,
    [RefundedAsProductFlag]      BIT             NULL,
    [TransactionSource]          VARCHAR (50)    NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

