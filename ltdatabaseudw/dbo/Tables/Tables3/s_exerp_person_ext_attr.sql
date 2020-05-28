CREATE TABLE [dbo].[s_exerp_person_ext_attr] (
    [s_exerp_person_ext_attr_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [person_id]                  VARCHAR (4000) NULL,
    [name]                       VARCHAR (4000) NULL,
    [value]                      VARCHAR (4000) NULL,
    [ets]                        BIGINT         NULL,
    [dummy_modified_date_time]   DATETIME       NULL,
    [dv_load_date_time]          DATETIME       NOT NULL,
    [dv_r_load_source_id]        BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL,
    [dv_hash]                    CHAR (32)      NOT NULL,
    [dv_deleted]                 BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_person_ext_attr]([dv_batch_id] ASC);

