﻿CREATE TABLE [dbo].[d_ltfeb_party_relationship] (
    [d_ltfeb_party_relationship_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [party_relationship_id]         INT          NULL,
    [from_party_role_id]            INT          NULL,
    [to_party_role_id]              INT          NULL,
    [effective_from_dim_date_key]   CHAR (8)     NULL,
    [effective_to_dim_date_key]     CHAR (8)     NULL,
    [p_ltfeb_party_relationship_id] BIGINT       NOT NULL,
    [deleted_flag]                  INT          NULL,
    [dv_load_date_time]             DATETIME     NULL,
    [dv_load_end_date_time]         DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ltfeb_party_relationship]([dv_batch_id] ASC);

