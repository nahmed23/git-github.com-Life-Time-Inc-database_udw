CREATE TABLE [dbo].[s_medallia_field] (
    [s_medallia_field_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [name_in_medallia]         VARCHAR (4000) NULL,
    [sr_no]                    VARCHAR (4000) NULL,
    [name_in_api]              VARCHAR (4000) NULL,
    [variable_name]            VARCHAR (4000) NULL,
    [description_question]     VARCHAR (4000) NULL,
    [data_type]                VARCHAR (4000) NULL,
    [single_select]            VARCHAR (4000) NULL,
    [examples]                 VARCHAR (4000) NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL,
    [dv_deleted]               BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

