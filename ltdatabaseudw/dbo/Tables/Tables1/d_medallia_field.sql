CREATE TABLE [dbo].[d_medallia_field] (
    [d_medallia_field_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [name_in_medallia]              VARCHAR (4000) NULL,
    [answer_id]                     VARCHAR (4000) NULL,
    [data_type]                     VARCHAR (4000) NULL,
    [description_question]          VARCHAR (4000) NULL,
    [dim_medallia_field_answer_key] VARCHAR (32)   NULL,
    [examples]                      VARCHAR (4000) NULL,
    [name_in_api]                   VARCHAR (4000) NULL,
    [single_select]                 VARCHAR (4000) NULL,
    [sr_no]                         VARCHAR (4000) NULL,
    [variable_name]                 VARCHAR (4000) NULL,
    [p_medallia_field_id]           BIGINT         NOT NULL,
    [deleted_flag]                  INT            NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

