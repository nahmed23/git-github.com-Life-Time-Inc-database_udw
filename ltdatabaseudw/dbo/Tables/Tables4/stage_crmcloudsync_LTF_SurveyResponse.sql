CREATE TABLE [dbo].[stage_crmcloudsync_LTF_SurveyResponse] (
    [stage_crmcloudsync_LTF_SurveyResponse_id] BIGINT          NOT NULL,
    [ltf_surveyresponseid]                     VARCHAR (36)    NULL,
    [ltf_survey_response]                      NVARCHAR (100)  NULL,
    [ltf_survey]                               VARCHAR (36)    NULL,
    [ltf_sequence]                             DECIMAL (18, 4) NULL,
    [ltf_question]                             NVARCHAR (1000) NULL,
    [ltf_response]                             NVARCHAR (1000) NULL,
    [statecode]                                INT             NULL,
    [statuscode]                               INT             NULL,
    [createdon]                                DATETIME        NULL,
    [createdby]                                VARCHAR (36)    NULL,
    [modifiedon]                               DATETIME        NULL,
    [modifiedby]                               VARCHAR (36)    NULL,
    [InsertedDateTime]                         DATETIME        NULL,
    [InsertUser]                               VARCHAR (100)   NULL,
    [UpdatedDateTime]                          DATETIME        NULL,
    [UpdateUser]                               VARCHAR (50)    NULL,
    [dv_batch_id]                              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

