CREATE TABLE [dbo].[stage_hash_medallia_field] (
    [stage_hash_medallia_field_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [name_in_medallia]             VARCHAR (4000) NULL,
    [sr_no]                        VARCHAR (4000) NULL,
    [name_in_api]                  VARCHAR (4000) NULL,
    [variable_name]                VARCHAR (4000) NULL,
    [answer_id]                    VARCHAR (4000) NULL,
    [description_question]         VARCHAR (4000) NULL,
    [data_type]                    VARCHAR (4000) NULL,
    [single_select]                VARCHAR (4000) NULL,
    [examples]                     VARCHAR (4000) NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

