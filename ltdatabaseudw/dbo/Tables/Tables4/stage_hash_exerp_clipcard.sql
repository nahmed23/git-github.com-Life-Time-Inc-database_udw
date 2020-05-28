CREATE TABLE [dbo].[stage_hash_exerp_clipcard] (
    [stage_hash_exerp_clipcard_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [id]                           VARCHAR (4000) NULL,
    [person_id]                    VARCHAR (4000) NULL,
    [company_id]                   VARCHAR (4000) NULL,
    [clips_left]                   INT            NULL,
    [clips_initial]                INT            NULL,
    [sale_log_id]                  VARCHAR (4000) NULL,
    [valid_from_datetime]          DATETIME       NULL,
    [valid_until_datetime]         DATETIME       NULL,
    [blocked]                      BIT            NULL,
    [cancelled]                    BIT            NULL,
    [cancel_datetime]              DATETIME       NULL,
    [assigned_person_id]           VARCHAR (4000) NULL,
    [center_id]                    INT            NULL,
    [ets]                          BIGINT         NULL,
    [comment]                      VARCHAR (4000) NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

