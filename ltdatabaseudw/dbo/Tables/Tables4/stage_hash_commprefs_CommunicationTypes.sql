CREATE TABLE [dbo].[stage_hash_commprefs_CommunicationTypes] (
    [stage_hash_commprefs_CommunicationTypes_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [Id]                                         INT            NULL,
    [Slug]                                       VARCHAR (8000) NULL,
    [Name]                                       VARCHAR (8000) NULL,
    [Description]                                VARCHAR (8000) NULL,
    [Sequence]                                   TINYINT        NULL,
    [ActiveOn]                                   DATETIME       NULL,
    [ActiveUntil]                                DATETIME       NULL,
    [CreatedTime]                                DATETIME       NULL,
    [UpdatedTime]                                DATETIME       NULL,
    [CommunicationCategoryId]                    INT            NULL,
    [OptInRequired]                              BIT            NULL,
    [SampleImageUrl]                             VARCHAR (8000) NULL,
    [dv_load_date_time]                          DATETIME       NOT NULL,
    [dv_inserted_date_time]                      DATETIME       NOT NULL,
    [dv_insert_user]                             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                       DATETIME       NULL,
    [dv_update_user]                             VARCHAR (50)   NULL,
    [dv_batch_id]                                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

