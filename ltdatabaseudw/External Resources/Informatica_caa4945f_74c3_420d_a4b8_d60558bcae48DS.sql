CREATE EXTERNAL DATA SOURCE [Informatica_caa4945f_74c3_420d_a4b8_d60558bcae48DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

