CREATE TABLE [dbo].[d_mms_department] (
    [d_mms_department_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [dim_mms_department_key] CHAR (32)    NULL,
    [department_id]          INT          NULL,
    [description]            VARCHAR (50) NULL,
    [name]                   VARCHAR (15) NULL,
    [sort_order]             CHAR (10)    NULL,
    [p_mms_department_id]    BIGINT       NOT NULL,
    [dv_load_date_time]      DATETIME     NULL,
    [dv_load_end_date_time]  DATETIME     NULL,
    [dv_batch_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_department]([dv_batch_id] ASC);

