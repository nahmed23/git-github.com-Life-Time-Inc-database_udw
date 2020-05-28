CREATE TABLE [dbo].[stage_hash_athlinks_api_vw_Course] (
    [stage_hash_athlinks_api_vw_Course_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [CourseID]                             INT             NULL,
    [RaceID]                               INT             NULL,
    [CourseName]                           NVARCHAR (255)  NULL,
    [RaceCatID]                            INT             NULL,
    [RaceCatDesc]                          NVARCHAR (50)   NULL,
    [CoursePatternID]                      INT             NULL,
    [CoursePattern]                        NVARCHAR (260)  NULL,
    [CoursePatternOuterID]                 INT             NULL,
    [CoursePatternOuterName]               NVARCHAR (260)  NULL,
    [OverallCount]                         INT             NULL,
    [EventCourseID]                        INT             NULL,
    [Settings]                             INT             NULL,
    [ResultsDate]                          DATETIME        NULL,
    [GalleryID]                            INT             NULL,
    [DistUnit]                             DECIMAL (18, 2) NULL,
    [DistTypeID]                           INT             NULL,
    [ResultsUser]                          INT             NULL,
    [CreateDate]                           DATETIME        NULL,
    [dv_load_date_time]                    DATETIME        NOT NULL,
    [dv_batch_id]                          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

