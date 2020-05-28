﻿CREATE TABLE [dbo].[stage_hash_mms_ACHChargeBackDetail] (
    [stage_hash_mms_ACHChargeBackDetail_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [RegionDescription]                         VARCHAR (50)    NULL,
    [ClubName]                                  VARCHAR (50)    NULL,
    [MemberID]                                  INT             NULL,
    [FirstName]                                 VARCHAR (50)    NULL,
    [LastName]                                  VARCHAR (50)    NULL,
    [ACH_CC]                                    VARCHAR (11)    NULL,
    [PaymentTypeDescription]                    VARCHAR (50)    NULL,
    [EFTDate]                                   VARCHAR (50)    NULL,
    [ReasonCodeDescription]                     VARCHAR (50)    NULL,
    [MembershipType_ProductDescription]         VARCHAR (50)    NULL,
    [ReturnCodeDescription]                     VARCHAR (50)    NULL,
    [EFTReturn_StopEFTFlag]                     VARCHAR (1)     NULL,
    [EFTReturn_RoutingNumber]                   VARCHAR (50)    NULL,
    [EFTReturn_AccountNumber]                   VARCHAR (50)    NULL,
    [EFTReturn_AccountExpirationDate]           VARCHAR (50)    NULL,
    [MembershipPhone]                           VARCHAR (14)    NULL,
    [EmailAddress]                              VARCHAR (140)   NULL,
    [ChargeBack_PostDateTime]                   VARCHAR (50)    NULL,
    [ChargeBack_MembershipEFTOptionDescription] VARCHAR (50)    NULL,
    [ChargeBack_MMSTranID]                      INT             NULL,
    [ChargeBack_TranAmount]                     DECIMAL (14, 4) NULL,
    [LocalCurrency_ChargeBack_TranAmount]       DECIMAL (14, 2) NULL,
    [USD_ChargeBack_TranAmount]                 DECIMAL (14, 4) NULL,
    [LocalCurrencyCode]                         VARCHAR (3)     NULL,
    [PlanRate]                                  DECIMAL (14, 4) NULL,
    [ReportingCurrencyCode]                     VARCHAR (3)     NULL,
    [EFTReturn_EFTAmount]                       DECIMAL (14, 4) NULL,
    [Membership_CurrentBalance]                 DECIMAL (14, 4) NULL,
    [LocalCurrency_EFTReturn_EFTAmount]         DECIMAL (14, 2) NULL,
    [LocalCurrency_Membership_CurrentBalance]   DECIMAL (14, 2) NULL,
    [USD_EFTReturn_EFTAmount]                   DECIMAL (14, 4) NULL,
    [USD_Membership_CurrentBalance]             DECIMAL (14, 4) NULL,
    [HeaderReturnType]                          VARCHAR (50)    NULL,
    [HeaderDateRange]                           VARCHAR (100)   NULL,
    [ReportRunDateTime]                         VARCHAR (50)    NULL,
    [jan_one]                                   DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

