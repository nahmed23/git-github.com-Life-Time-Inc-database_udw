CREATE TABLE [dbo].[d_affinitech_cameras] (
    [d_affinitech_cameras_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)     NOT NULL,
    [cam_id]                  VARCHAR (255) NULL,
    [cam_club_it]             VARCHAR (255) NULL,
    [cam_dim_club_key]        VARCHAR (32)  NULL,
    [cam_inverted]            INT           NULL,
    [cam_ip]                  VARCHAR (255) NULL,
    [cam_name]                VARCHAR (255) NULL,
    [studio]                  VARCHAR (255) NULL,
    [p_affinitech_cameras_id] BIGINT        NOT NULL,
    [deleted_flag]            INT           NULL,
    [dv_load_date_time]       DATETIME      NULL,
    [dv_load_end_date_time]   DATETIME      NULL,
    [dv_batch_id]             BIGINT        NOT NULL,
    [dv_inserted_date_time]   DATETIME      NOT NULL,
    [dv_insert_user]          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]    DATETIME      NULL,
    [dv_update_user]          VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

