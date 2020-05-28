CREATE TABLE [dbo].[stage_hash_exerp_activity_group] (
    [stage_hash_exerp_activity_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [id]                                 INT            NULL,
    [name]                               VARCHAR (4000) NULL,
    [state]                              VARCHAR (4000) NULL,
    [book_kiosk]                         INT            NULL,
    [book_web]                           INT            NULL,
    [book_api]                           INT            NULL,
    [book_mobile_api]                    INT            NULL,
    [book_client]                        INT            NULL,
    [parent_activity_group_id]           INT            NULL,
    [external_id]                        VARCHAR (200)  NULL,
    [dummy_modified_date_time]           DATETIME       NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

