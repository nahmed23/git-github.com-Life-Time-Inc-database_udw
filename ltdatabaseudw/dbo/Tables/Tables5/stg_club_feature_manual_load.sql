CREATE TABLE [dbo].[stg_club_feature_manual_load] (
    [dim_club_key]     VARCHAR (32)  NOT NULL,
    [formal_club_name] VARCHAR (255) NULL,
    [club_name]        VARCHAR (255) NULL,
    [club_id]          INT           NOT NULL,
    [club_type]        VARCHAR (21)  NULL,
    [display_name]     VARCHAR (255) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

