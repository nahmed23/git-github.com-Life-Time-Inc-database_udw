﻿CREATE TABLE [dbo].[stage_boss_asiclubres] (
    [stage_boss_asiclubres_id] BIGINT        NOT NULL,
    [club]                     INT           NULL,
    [resource_id]              INT           NULL,
    [status]                   CHAR (1)      NULL,
    [default_upccode]          CHAR (15)     NULL,
    [empl_id]                  CHAR (6)      NULL,
    [display_seq]              SMALLINT      NULL,
    [resource_type]            CHAR (25)     NULL,
    [resource]                 CHAR (25)     NULL,
    [comment]                  VARCHAR (40)  NULL,
    [resource_type_id]         SMALLINT      NULL,
    [square_feet]              INT           NULL,
    [employee_id]              INT           NULL,
    [created_at]               DATETIME      NULL,
    [updated_at]               DATETIME      NULL,
    [capacity]                 INT           NULL,
    [web_enable]               CHAR (1)      NULL,
    [web_start_date]           DATETIME      NULL,
    [web_active]               CHAR (1)      NULL,
    [inactive_start_date]      DATETIME      NULL,
    [inactive_end_date]        DATETIME      NULL,
    [phone]                    VARCHAR (12)  NULL,
    [floor]                    INT           NULL,
    [web_description]          VARCHAR (400) NULL,
    [supportPhone]             VARCHAR (12)  NULL,
    [supportEmail]             VARCHAR (240) NULL,
    [resourcePhone]            VARCHAR (12)  NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

