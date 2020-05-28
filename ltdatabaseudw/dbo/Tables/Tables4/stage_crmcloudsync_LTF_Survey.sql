CREATE TABLE [dbo].[stage_crmcloudsync_LTF_Survey] (
    [stage_crmcloudsync_LTF_Survey_id] BIGINT         NOT NULL,
    [ltf_surveyid]                     VARCHAR (36)   NULL,
    [ltf_name]                         NVARCHAR (100) NULL,
    [ltf_surveytype]                   INT            NULL,
    [ltf_membernumber]                 NVARCHAR (100) NULL,
    [ltf_employeeid]                   NVARCHAR (100) NULL,
    [ltf_subscriber]                   VARCHAR (36)   NULL,
    [ltf_surveytoolid]                 NVARCHAR (100) NULL,
    [ltf_submittedon]                  DATETIME       NULL,
    [ltf_source]                       INT            NULL,
    [statecode]                        INT            NULL,
    [statuscode]                       INT            NULL,
    [ltf_submittedby]                  VARCHAR (36)   NULL,
    [createdon]                        DATETIME       NULL,
    [createdby]                        VARCHAR (36)   NULL,
    [modifiedon]                       DATETIME       NULL,
    [modifiedby]                       VARCHAR (36)   NULL,
    [InsertedDateTime]                 DATETIME       NULL,
    [InsertUser]                       VARCHAR (100)  NULL,
    [UpdatedDateTime]                  DATETIME       NULL,
    [UpdateUser]                       VARCHAR (50)   NULL,
    [ltf_connectmember]                VARCHAR (36)   NULL,
    [dv_batch_id]                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

