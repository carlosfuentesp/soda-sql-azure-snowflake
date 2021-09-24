# soda-sql-azure-snowflake
Testing data from Snowflake using Soda SQL in Azure functions.

This is a small POC that connect an Azure Function written in Python to Snowflake. It reads all tables for a specific warehouse and checks data duplication in each of the tables. It uses a programatically YAML dictionary way to read scan and connection configuration.


For more detaiuls about Soda-sql please refer to Soda-sql https://docs.soda.io/soda-sql/concepts.html
