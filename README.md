# SQLScripting

This repository hosts SQL Server scripts for targeted database maintenance. The primary stored procedure, `usp_UpdateTargetedStatistics`, updates table statistics when enough rows have changed.

## Deployment

Run `stored-procedures/usp_UpdateTargetedStatistics.sql` in the target database to create the procedure in the `dbo` schema.
