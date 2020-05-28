CREATE TABLE [dbo].[s_crmcloudsync_ltf_survey_response] (
    [s_crmcloudsync_ltf_survey_response_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [ltf_survey_response_id]                VARCHAR (36)    NULL,
    [ltf_survey_response]                   NVARCHAR (100)  NULL,
    [ltf_sequence]                          DECIMAL (18, 4) NULL,
    [ltf_question]                          NVARCHAR (1000) NULL,
    [ltf_response]                          NVARCHAR (1000) NULL,
    [created_on]                            DATETIME        NULL,
    [modified_on]                           DATETIME        NULL,
    [inserted_date_time]                    DATETIME        NULL,
    [insert_user]                           VARCHAR (100)   NULL,
    [updated_date_time]                     DATETIME        NULL,
    [update_user]                           VARCHAR (50)    NULL,
    [state_code]                            INT             NULL,
    [status_code]                           INT             NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_r_load_source_id]                   BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL,
    [dv_hash]                               CHAR (32)       NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_ltf_survey_response]([dv_batch_id] ASC);

