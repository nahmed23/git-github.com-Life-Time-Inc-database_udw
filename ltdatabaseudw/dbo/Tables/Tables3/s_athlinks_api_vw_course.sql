CREATE TABLE [dbo].[s_athlinks_api_vw_course] (
    [s_athlinks_api_vw_course_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [course_id]                   INT             NULL,
    [course_name]                 NVARCHAR (255)  NULL,
    [race_cat_desc]               NVARCHAR (50)   NULL,
    [course_pattern]              NVARCHAR (260)  NULL,
    [course_pattern_outer_name]   NVARCHAR (260)  NULL,
    [overall_count]               INT             NULL,
    [settings]                    INT             NULL,
    [results_date]                DATETIME        NULL,
    [gallery_id]                  INT             NULL,
    [dist_unit]                   DECIMAL (18, 2) NULL,
    [dist_type_id]                INT             NULL,
    [results_user]                INT             NULL,
    [create_date]                 DATETIME        NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL,
    [dv_deleted]                  BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

