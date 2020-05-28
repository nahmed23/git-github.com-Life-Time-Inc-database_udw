CREATE EXTERNAL DATA SOURCE [Informatica_dd4682ce_72d6_48de_862c_c60c5082c6d4DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

