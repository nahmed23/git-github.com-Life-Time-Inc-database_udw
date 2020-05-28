CREATE TABLE [dbo].[d_medallia_field_answer] (
    [d_medallia_field_answer_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [answer_id]                     VARCHAR (4000) NULL,
    [answer_name]                   VARCHAR (4000) NULL,
    [answer_type]                   VARCHAR (4000) NULL,
    [dim_medallia_field_answer_key] VARCHAR (32)   NULL,
    [p_medallia_field_answer_id]    BIGINT         NOT NULL,
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

