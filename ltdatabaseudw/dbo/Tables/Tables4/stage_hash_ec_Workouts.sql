CREATE TABLE [dbo].[stage_hash_ec_Workouts] (
    [stage_hash_ec_Workouts_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [Id]                        INT            NULL,
    [Name]                      NVARCHAR (100) NULL,
    [Description]               VARCHAR (8000) NULL,
    [CreatedDate]               DATETIME       NULL,
    [ModifiedDate]              DATETIME       NULL,
    [InactiveDate]              DATETIME       NULL,
    [Tags]                      VARCHAR (8000) NULL,
    [PartyId]                   INT            NULL,
    [Discriminator]             NVARCHAR (128) NULL,
    [Type]                      INT            NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

