CREATE TABLE [dbo].[s_affinitech_camera_count] (
    [s_affinitech_camera_count_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [Door_Description]             VARCHAR (50)  NULL,
    [Start_Range]                  DATETIME      NULL,
    [Source_IP]                    VARCHAR (50)  NULL,
    [Event_Type]                   INT           NULL,
    [Door_Type]                    INT           NULL,
    [Enters]                       INT           NULL,
    [Exits]                        INT           NULL,
    [Cumulative_Enters]            INT           NULL,
    [Cumulative_Exits]             INT           NULL,
    [File_Name]                    VARCHAR (255) NULL,
    [Inserted_DateTime]            DATETIME      NULL,
    [dv_load_date_time]            DATETIME      NOT NULL,
    [dv_r_load_source_id]          BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL,
    [dv_hash]                      CHAR (32)     NOT NULL,
    [dv_deleted]                   BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

