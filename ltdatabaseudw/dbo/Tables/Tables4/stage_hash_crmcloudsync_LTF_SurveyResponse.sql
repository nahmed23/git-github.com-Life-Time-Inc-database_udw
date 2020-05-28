CREATE TABLE [dbo].[stage_hash_crmcloudsync_LTF_SurveyResponse] (
    [stage_hash_crmcloudsync_LTF_SurveyResponse_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)       NOT NULL,
    [ltf_surveyresponseid]                          VARCHAR (36)    NULL,
    [ltf_survey_response]                           NVARCHAR (100)  NULL,
    [ltf_survey]                                    VARCHAR (36)    NULL,
    [ltf_sequence]                                  DECIMAL (18, 4) NULL,
    [ltf_question]                                  NVARCHAR (1000) NULL,
    [ltf_response]                                  NVARCHAR (1000) NULL,
    [statecode]                                     INT             NULL,
    [statuscode]                                    INT             NULL,
    [createdon]                                     DATETIME        NULL,
    [createdby]                                     VARCHAR (36)    NULL,
    [modifiedon]                                    DATETIME        NULL,
    [modifiedby]                                    VARCHAR (36)    NULL,
    [InsertedDateTime]                              DATETIME        NULL,
    [InsertUser]                                    VARCHAR (100)   NULL,
    [UpdatedDateTime]                               DATETIME        NULL,
    [UpdateUser]                                    VARCHAR (50)    NULL,
    [dv_load_date_time]                             DATETIME        NOT NULL,
    [dv_updated_date_time]                          DATETIME        NULL,
    [dv_update_user]                                VARCHAR (50)    NULL,
    [dv_batch_id]                                   BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

