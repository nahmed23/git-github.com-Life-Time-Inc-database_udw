CREATE EXTERNAL DATA SOURCE [Informatica_8e6239c3_d535_4bdf_bae2_dfd794ea4c88DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

