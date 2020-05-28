CREATE TABLE [dbo].[l_mms_package_adjustment] (
    [l_mms_package_adjustment_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [package_adjustment_id]          INT          NULL,
    [package_id]                     INT          NULL,
    [employee_id]                    INT          NULL,
    [mms_tran_id]                    INT          NULL,
    [val_package_adjustment_type_id] SMALLINT     NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_package_adjustment]
    ON [dbo].[l_mms_package_adjustment]([bk_hash] ASC, [l_mms_package_adjustment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_package_adjustment]([dv_batch_id] ASC);

