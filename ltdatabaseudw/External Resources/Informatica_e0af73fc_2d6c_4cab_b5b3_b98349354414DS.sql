CREATE EXTERNAL DATA SOURCE [Informatica_e0af73fc_2d6c_4cab_b5b3_b98349354414DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

