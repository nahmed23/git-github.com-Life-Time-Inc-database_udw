CREATE EXTERNAL DATA SOURCE [Informatica_f53958a8_55cd_4ddf_94d9_68e28a622563DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

