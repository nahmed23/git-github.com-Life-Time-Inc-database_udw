CREATE EXTERNAL DATA SOURCE [Informatica_805266aa_b39c_4634_b9e4_83df480f214aDS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

