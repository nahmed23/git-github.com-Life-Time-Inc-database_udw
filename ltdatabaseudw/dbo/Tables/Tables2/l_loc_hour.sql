﻿CREATE TABLE [dbo].[l_loc_hour] (
    [l_loc_hour_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [hour_id]               BIGINT       NULL,
    [location_id]           BIGINT       NULL,
    [val_hour_type_id]      BIGINT       NULL,
    [udw_dim_location_key]  VARCHAR (32) NULL,
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

