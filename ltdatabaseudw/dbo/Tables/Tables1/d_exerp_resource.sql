CREATE TABLE [dbo].[d_exerp_resource] (
    [d_exerp_resource_id]        BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [resource_id]                VARCHAR (4000) NULL,
    [access_group_id]            VARCHAR (4000) NULL,
    [comment]                    VARCHAR (4000) NULL,
    [d_exerp_center_bk_hash]     CHAR (32)      NULL,
    [external_id]                VARCHAR (4000) NULL,
    [resource_access_group_name] VARCHAR (4000) NULL,
    [resource_name]              VARCHAR (4000) NULL,
    [resource_state]             VARCHAR (4000) NULL,
    [resource_type]              VARCHAR (4000) NULL,
    [show_calendar]              CHAR (1)       NULL,
    [p_exerp_resource_id]        BIGINT         NOT NULL,
    [deleted_flag]               INT            NULL,
    [dv_load_date_time]          DATETIME       NULL,
    [dv_load_end_date_time]      DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_resource]([dv_batch_id] ASC);

