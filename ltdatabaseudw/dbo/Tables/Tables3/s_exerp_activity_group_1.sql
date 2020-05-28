﻿CREATE TABLE [dbo].[s_exerp_activity_group_1] (
    [s_exerp_activity_group_1_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)     NOT NULL,
    [activity_group_id]           INT           NULL,
    [external_id]                 VARCHAR (200) NULL,
    [dummy_modified_date_time]    DATETIME      NULL,
    [dv_load_date_time]           DATETIME      NOT NULL,
    [dv_r_load_source_id]         BIGINT        NOT NULL,
    [dv_inserted_date_time]       DATETIME      NOT NULL,
    [dv_insert_user]              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]        DATETIME      NULL,
    [dv_update_user]              VARCHAR (50)  NULL,
    [dv_hash]                     CHAR (32)     NOT NULL,
    [dv_deleted]                  BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

