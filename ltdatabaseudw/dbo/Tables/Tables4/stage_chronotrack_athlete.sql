﻿CREATE TABLE [dbo].[stage_chronotrack_athlete] (
    [stage_chronotrack_athlete_id] BIGINT         NOT NULL,
    [id]                           BIGINT         NULL,
    [account_id]                   BIGINT         NULL,
    [first_name]                   NVARCHAR (40)  NULL,
    [middle_name]                  NVARCHAR (40)  NULL,
    [last_name]                    NVARCHAR (40)  NULL,
    [name_pronunciation]           NVARCHAR (255) NULL,
    [sex]                          NCHAR (13)     NULL,
    [birthdate]                    DATE           NULL,
    [age]                          TINYINT        NULL,
    [tshirt_size]                  NVARCHAR (15)  NULL,
    [usat_num]                     NVARCHAR (36)  NULL,
    [location_id]                  BIGINT         NULL,
    [home_phone]                   NVARCHAR (20)  NULL,
    [mobile_phone]                 NVARCHAR (20)  NULL,
    [email]                        NVARCHAR (255) NULL,
    [emerg_name]                   NVARCHAR (50)  NULL,
    [emerg_phone]                  NVARCHAR (20)  NULL,
    [emerg_relationship]           NVARCHAR (255) NULL,
    [medical_notes]                VARCHAR (4000) NULL,
    [ctime]                        INT            NULL,
    [mtime]                        INT            NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

