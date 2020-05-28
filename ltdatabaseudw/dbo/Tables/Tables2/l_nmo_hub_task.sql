CREATE TABLE [dbo].[l_nmo_hub_task] (
    [l_nmo_hub_task_id]      BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [id]                     INT          NULL,
    [hub_task_department_id] INT          NULL,
    [hub_task_status_id]     INT          NULL,
    [hub_task_type_id]       INT          NULL,
    [club_id]                INT          NULL,
    [party_id]               INT          NULL,
    [creator_party_id]       INT          NULL,
    [assignee_party_id]      INT          NULL,
    [dv_load_date_time]      DATETIME     NOT NULL,
    [dv_r_load_source_id]    BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL,
    [dv_hash]                CHAR (32)    NOT NULL,
    [dv_deleted]             BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

