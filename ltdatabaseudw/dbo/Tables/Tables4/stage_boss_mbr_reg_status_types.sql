CREATE TABLE [dbo].[stage_boss_mbr_reg_status_types] (
    [stage_boss_mbr_reg_status_types_id] BIGINT        NOT NULL,
    [id]                                 INT           NULL,
    [name]                               VARCHAR (50)  NULL,
    [description]                        VARCHAR (100) NULL,
    [position]                           INT           NULL,
    [term_type]                          CHAR (1)      NULL,
    [active]                             CHAR (1)      NULL,
    [created_at]                         DATETIME      NULL,
    [updated_at]                         DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

