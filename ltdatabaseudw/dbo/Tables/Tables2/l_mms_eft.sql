CREATE TABLE [dbo].[l_mms_eft] (
    [l_mms_eft_id]          BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [eft_id]                INT          NULL,
    [membership_id]         INT          NULL,
    [val_eft_status_id]     TINYINT      NULL,
    [eft_return_code_id]    INT          NULL,
    [payment_id]            INT          NULL,
    [val_eft_type_id]       TINYINT      NULL,
    [val_payment_type_id]   TINYINT      NULL,
    [member_id]             INT          NULL,
    [job_task_id]           INT          NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_deleted]            BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

