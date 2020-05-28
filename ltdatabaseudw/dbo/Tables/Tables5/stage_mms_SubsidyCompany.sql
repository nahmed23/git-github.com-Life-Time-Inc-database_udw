CREATE TABLE [dbo].[stage_mms_SubsidyCompany] (
    [stage_mms_SubsidyCompany_id]  BIGINT         NOT NULL,
    [SubsidyCompanyID]             INT            NULL,
    [CompanyID]                    INT            NULL,
    [Description]                  VARCHAR (255)  NULL,
    [LTFEmailDistributionList]     VARCHAR (1000) NULL,
    [PartnerEmailDistributionList] VARCHAR (1000) NULL,
    [InsertedDateTime]             DATETIME       NULL,
    [UpdatedDateTime]              DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

