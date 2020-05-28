CREATE TABLE [dbo].[stage_hash_boss_asicolorr] (
    [stage_hash_boss_asicolorr_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [colorr_dept]                  INT          NULL,
    [colorr_class]                 INT          NULL,
    [colorr_code]                  CHAR (8)     NULL,
    [colorr_desc]                  CHAR (30)    NULL,
    [colorr_seq]                   SMALLINT     NULL,
    [colorr_class_id]              INT          NULL,
    [id]                           INT          NULL,
    [jan_one]                      DATETIME     NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

