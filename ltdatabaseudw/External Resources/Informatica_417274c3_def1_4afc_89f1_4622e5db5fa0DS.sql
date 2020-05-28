CREATE EXTERNAL DATA SOURCE [Informatica_417274c3_def1_4afc_89f1_4622e5db5fa0DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

