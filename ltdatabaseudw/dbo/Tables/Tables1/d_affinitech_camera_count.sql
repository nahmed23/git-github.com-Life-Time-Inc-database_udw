CREATE TABLE [dbo].[d_affinitech_camera_count] (
    [d_affinitech_camera_count_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [Door_Description]             VARCHAR (50) NULL,
    [Start_Range]                  DATETIME     NULL,
    [Source_IP]                    VARCHAR (50) NULL,
    [Cumulative_Enters]            INT          NULL,
    [Cumulative_Exits]             INT          NULL,
    [Division_ID]                  VARCHAR (50) NULL,
    [Door_ID]                      VARCHAR (50) NULL,
    [Door_Type]                    INT          NULL,
    [Enters]                       INT          NULL,
    [Event_Type]                   INT          NULL,
    [Exits]                        INT          NULL,
    [Site_ID]                      VARCHAR (50) NULL,
    [Start_Range_dim_date_key]     VARCHAR (8)  NULL,
    [Start_Range_dim_time_key]     VARCHAR (8)  NULL,
    [p_affinitech_camera_count_id] BIGINT       NOT NULL,
    [deleted_flag]                 INT          NULL,
    [dv_load_date_time]            DATETIME     NULL,
    [dv_load_end_date_time]        DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

