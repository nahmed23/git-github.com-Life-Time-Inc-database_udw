CREATE TABLE [dbo].[stage_hash_nmo_hubtasknotes] (
    [stage_hash_nmo_hubtasknotes_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)      NOT NULL,
    [id]                             INT            NULL,
    [hubtaskid]                      INT            NULL,
    [title]                          NVARCHAR (255) NULL,
    [description]                    VARCHAR (8000) NULL,
    [creatorpartyid]                 INT            NULL,
    [creatorname]                    NVARCHAR (60)  NULL,
    [createddate]                    DATETIME       NULL,
    [updateddate]                    DATETIME       NULL,
    [dv_load_date_time]              DATETIME       NOT NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

