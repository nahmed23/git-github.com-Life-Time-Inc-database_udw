﻿CREATE TABLE [dbo].[r_segment_val_star_rank] (
    [r_segment_val_star_rank_id] INT          IDENTITY (1, 1) NOT NULL,
    [val_star_rank_id]           INT          NOT NULL,
    [description]                VARCHAR (50) NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_load_end_date_time]      DATETIME     NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL,
    [dv_deleted]                 BIT          NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

