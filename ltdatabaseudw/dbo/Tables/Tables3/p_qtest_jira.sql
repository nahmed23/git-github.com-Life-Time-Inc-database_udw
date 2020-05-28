CREATE TABLE [dbo].[p_qtest_jira] (
    [p_qtest_jira_id]                      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [project_id]                           VARCHAR (4000) NULL,
    [release_id]                           VARCHAR (4000) NULL,
    [requirement_id]                       VARCHAR (4000) NULL,
    [test_case_id]                         VARCHAR (4000) NULL,
    [l_qtest_jira_id]                      BIGINT         NULL,
    [s_qtest_jira_id]                      BIGINT         NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_load_end_date_time]                DATETIME       NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME       NULL,
    [dv_next_greatest_satellite_date_time] DATETIME       NULL,
    [dv_first_in_key_series]               INT            NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

