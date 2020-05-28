CREATE TABLE [dbo].[stage_hash_hybris_units] (
    [stage_hash_hybris_units_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [hjmpTS]                     BIGINT          NULL,
    [createdTS]                  DATETIME        NULL,
    [modifiedTS]                 DATETIME        NULL,
    [TypePkString]               BIGINT          NULL,
    [OwnerPkString]              BIGINT          NULL,
    [PK]                         BIGINT          NULL,
    [p_code]                     NVARCHAR (255)  NULL,
    [p_conversion]               DECIMAL (26, 6) NULL,
    [p_unittype]                 NVARCHAR (255)  NULL,
    [aCLTS]                      BIGINT          NULL,
    [propTS]                     BIGINT          NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

