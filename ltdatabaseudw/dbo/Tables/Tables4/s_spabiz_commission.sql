﻿CREATE TABLE [dbo].[s_spabiz_commission] (
    [s_spabiz_commission_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [commission_id]          DECIMAL (26, 6) NULL,
    [counter_id]             DECIMAL (26, 6) NULL,
    [edit_time]              DATETIME        NULL,
    [commission_delete]      DECIMAL (26, 6) NULL,
    [delete_date]            DATETIME        NULL,
    [name]                   VARCHAR (150)   NULL,
    [use_sliding_scale]      DECIMAL (26, 6) NULL,
    [level_1_value]          DECIMAL (26, 6) NULL,
    [level_1_commish]        DECIMAL (26, 6) NULL,
    [level_2_value]          DECIMAL (26, 6) NULL,
    [level_2_commish]        DECIMAL (26, 6) NULL,
    [level_3_value]          DECIMAL (26, 6) NULL,
    [level_3_commish]        DECIMAL (26, 6) NULL,
    [level_4_value]          DECIMAL (26, 6) NULL,
    [level_4_commish]        DECIMAL (26, 6) NULL,
    [level_5_value]          DECIMAL (26, 6) NULL,
    [level_5_commish]        DECIMAL (26, 6) NULL,
    [store_number]           DECIMAL (26, 6) NULL,
    [commission_backup_id]   DECIMAL (26, 6) NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_commission]
    ON [dbo].[s_spabiz_commission]([bk_hash] ASC, [s_spabiz_commission_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_commission]([dv_batch_id] ASC);

