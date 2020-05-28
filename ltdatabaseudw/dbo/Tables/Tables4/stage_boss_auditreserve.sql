CREATE TABLE [dbo].[stage_boss_auditreserve] (
    [stage_boss_auditreserve_id] BIGINT       NOT NULL,
    [id]                         INT          NULL,
    [reservation]                INT          NULL,
    [club]                       INT          NULL,
    [start_date]                 DATETIME     NULL,
    [create_date]                DATETIME     NULL,
    [upc_desc]                   VARCHAR (50) NULL,
    [reservation_type]           CHAR (1)     NULL,
    [audit_type]                 VARCHAR (50) NULL,
    [audit_performed]            DATETIME     NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

