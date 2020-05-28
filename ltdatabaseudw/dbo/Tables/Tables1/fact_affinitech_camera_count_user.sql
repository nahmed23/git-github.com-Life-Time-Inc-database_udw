CREATE TABLE [dbo].[fact_affinitech_camera_count_user] (
    [club_name]            VARCHAR (50)   NULL,
    [club_code]            VARCHAR (18)   NULL,
    [class_date]           DATE           NULL,
    [resource]             VARCHAR (4000) NULL,
    [upc_code]             VARCHAR (4000) NULL,
    [upc_desc]             VARCHAR (4000) NULL,
    [booking_reference_id] VARCHAR (4000) NULL,
    [booking_instance_id]  VARCHAR (4000) NULL,
    [start_time]           TIME (7)       NULL,
    [end_time]             TIME (7)       NULL,
    [instructor_count]     INT            NULL,
    [camera_count]         INT            NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([booking_reference_id]));

