﻿CREATE TABLE [dbo].[l_exerp_access_privilege] (
    [l_exerp_access_privilege_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [access_privilege_id]         INT          NULL,
    [privilege_set_id]            INT          NULL,
    [access_group_id]             INT          NULL,
    [scope_id]                    INT          NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_r_load_source_id]         BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_hash]                     CHAR (32)    NOT NULL,
    [dv_deleted]                  BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

