CREATE TABLE [dbo].[stage_hash_affinitech_camera_count] (
    [stage_hash_affinitech_camera_count_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [DoorDescription]                       VARCHAR (50)  NULL,
    [StartRange]                            DATETIME      NULL,
    [SourceIP]                              VARCHAR (50)  NULL,
    [EventType]                             INT           NULL,
    [DivisionID]                            VARCHAR (50)  NULL,
    [SiteID]                                VARCHAR (50)  NULL,
    [DoorID]                                VARCHAR (50)  NULL,
    [DoorType]                              INT           NULL,
    [Enters]                                INT           NULL,
    [Exits]                                 INT           NULL,
    [CumulativeEnters]                      INT           NULL,
    [CumulativeExits]                       INT           NULL,
    [FileName]                              VARCHAR (255) NULL,
    [InsertedDateTime]                      DATETIME      NULL,
    [dv_load_date_time]                     DATETIME      NOT NULL,
    [dv_batch_id]                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

