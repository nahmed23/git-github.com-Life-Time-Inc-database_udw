CREATE TABLE [dbo].[stg_attribute_note] (
    [dim_club_key] VARCHAR (32)   NOT NULL,
    [club_id]      INT            NOT NULL,
    [club_name]    VARCHAR (255)  NULL,
    [display_name] VARCHAR (255)  NULL,
    [notes]        VARCHAR (8000) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

