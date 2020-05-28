CREATE TABLE [dbo].[stage_commprefs_CommunicationTypes] (
    [stage_commprefs_CommunicationTypes_id] BIGINT         NOT NULL,
    [Id]                                    INT            NULL,
    [Slug]                                  VARCHAR (8000) NULL,
    [Name]                                  VARCHAR (8000) NULL,
    [Description]                           VARCHAR (8000) NULL,
    [Sequence]                              TINYINT        NULL,
    [ActiveOn]                              DATETIME       NULL,
    [ActiveUntil]                           DATETIME       NULL,
    [CreatedTime]                           DATETIME       NULL,
    [UpdatedTime]                           DATETIME       NULL,
    [CommunicationCategoryId]               INT            NULL,
    [OptInRequired]                         BIT            NULL,
    [SampleImageUrl]                        VARCHAR (8000) NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

