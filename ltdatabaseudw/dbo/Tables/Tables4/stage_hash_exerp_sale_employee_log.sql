﻿CREATE TABLE [dbo].[stage_hash_exerp_sale_employee_log] (
    [stage_hash_exerp_sale_employee_log_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)      NOT NULL,
    [id]                                    INT            NULL,
    [sale_id]                               VARCHAR (4000) NULL,
    [sale_person_id]                        VARCHAR (4000) NULL,
    [change_person_id]                      VARCHAR (4000) NULL,
    [from_datetime]                         DATETIME       NULL,
    [center_id]                             INT            NULL,
    [ets]                                   BIGINT         NULL,
    [dummy_modified_date_time]              DATETIME       NULL,
    [dv_load_date_time]                     DATETIME       NOT NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

