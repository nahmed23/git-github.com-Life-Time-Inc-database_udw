CREATE TABLE [dbo].[identity_test] (
    [id]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash] CHAR (32)     NULL,
    [v]       VARCHAR (500) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

