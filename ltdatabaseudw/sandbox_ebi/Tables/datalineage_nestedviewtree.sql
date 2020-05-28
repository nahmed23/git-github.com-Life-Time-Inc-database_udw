CREATE TABLE [sandbox_ebi].[datalineage_nestedviewtree] (
    [eval_lvl]      INT            NOT NULL,
    [TopViewSchema] NVARCHAR (128) NULL,
    [TopViewName]   [sysname]      NOT NULL,
    [view_schema]   NVARCHAR (128) NULL,
    [view_name]     [sysname]      NOT NULL,
    [table_schema]  [sysname]      NULL,
    [table_name]    [sysname]      NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

