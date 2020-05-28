﻿CREATE TABLE [dbo].[wrk_pega_member_usage] (
    [wrk_pega_member_usage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [check_in_date_time]       DATETIME     NULL,
    [club_id]                  INT          NULL,
    [dim_mms_member_key]       VARCHAR (32) NULL,
    [member_id]                INT          NULL,
    [member_usage_id]          INT          NULL,
    [sequence_number]          INT          NULL,
    [dv_load_date_time]        DATETIME     NULL,
    [dv_load_end_date_time]    DATETIME     NULL,
    [dv_batch_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

