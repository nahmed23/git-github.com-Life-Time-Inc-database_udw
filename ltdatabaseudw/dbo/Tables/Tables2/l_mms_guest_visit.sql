CREATE TABLE [dbo].[l_mms_guest_visit] (
    [l_mms_guest_visit_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [guest_visit_id]             INT          NULL,
    [guest_id]                   INT          NULL,
    [club_id]                    INT          NULL,
    [val_guest_access_method_id] INT          NULL,
    [member_id]                  INT          NULL,
    [employee_id]                INT          NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_guest_visit]
    ON [dbo].[l_mms_guest_visit]([bk_hash] ASC, [l_mms_guest_visit_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_guest_visit]([dv_batch_id] ASC);

