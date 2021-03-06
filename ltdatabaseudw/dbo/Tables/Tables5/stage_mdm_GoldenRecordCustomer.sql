﻿CREATE TABLE [dbo].[stage_mdm_GoldenRecordCustomer] (
    [stage_mdm_GoldenRecordCustomer_id] BIGINT        NOT NULL,
    [LoadDateTime]                      DATETIME      NULL,
    [RowNumber]                         INT           NULL,
    [EntityID]                          VARCHAR (128) NULL,
    [SourceID]                          VARCHAR (128) NULL,
    [SourceCode]                        VARCHAR (128) NULL,
    [BirthDate]                         VARCHAR (19)  NULL,
    [ContactID]                         VARCHAR (128) NULL,
    [CreateDate]                        VARCHAR (19)  NULL,
    [TerminateDate]                     VARCHAR (128) NULL,
    [Email1]                            VARCHAR (128) NULL,
    [Email2]                            VARCHAR (128) NULL,
    [Sex]                               VARCHAR (128) NULL,
    [PostalAddressCity]                 VARCHAR (50)  NULL,
    [PostalAddressState]                VARCHAR (15)  NULL,
    [PostalAddressLine1]                VARCHAR (75)  NULL,
    [PostalAddressLine2]                VARCHAR (75)  NULL,
    [PostalAddressZipCode]              VARCHAR (10)  NULL,
    [IPAddress]                         VARCHAR (128) NULL,
    [LeadID]                            VARCHAR (128) NULL,
    [MemberID]                          VARCHAR (128) NULL,
    [FirstName]                         VARCHAR (30)  NULL,
    [LastName]                          VARCHAR (75)  NULL,
    [MiddleName]                        VARCHAR (30)  NULL,
    [PrefixName]                        VARCHAR (10)  NULL,
    [SuffixName]                        VARCHAR (10)  NULL,
    [PartyID]                           VARCHAR (128) NULL,
    [Phone1]                            VARCHAR (40)  NULL,
    [Phone2]                            VARCHAR (40)  NULL,
    [MembershipID]                      VARCHAR (128) NULL,
    [SPACustomerID]                     VARCHAR (128) NULL,
    [UpdateDate]                        VARCHAR (19)  NULL,
    [ActivationDate]                    VARCHAR (128) NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

