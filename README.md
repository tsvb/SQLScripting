# SQLScripting

This repository contains SQL Server scripts including the stored procedure `usp_UpdateTargetedStatistics`.

## Stored Procedures

### `dbo.usp_UpdateTargetedStatistics`

The script `stored-procedures/usp_UpdateTargetedStatistics.sql` creates `dbo.usp_UpdateTargetedStatistics`. It updates statistics in a specified database when enough rows have been modified.

#### Parameters

- `@DatabaseName` **sysname** *(required)*: target database name.
- `@MinRowCount` **BIGINT**: ignore tables with fewer rows. Default `100000`.
- `@MinModificationPct` **DECIMAL(10,4)**: percentage of rows modified before a table or statistic is eligible. Default `10.0`.
- `@ExecuteUpdates` **BIT**: `0` performs a dry run and only lists commands; `1` executes `UPDATE STATISTICS`. Default `0`.
- `@UseFullScan` **BIT**: when `1`, statistics are updated with `FULLSCAN`; when `0`, `RESAMPLE` is used. Default `0`.
- `@Granularity` **CHAR(5)**: `'TABLE'` updates per table, `'STAT'` updates each statistic individually. Default `'TABLE'`.

#### Usage examples

1. **Dry run** — list pending updates without executing:

```sql
EXEC dbo.usp_UpdateTargetedStatistics
    @DatabaseName = 'MyDatabase';
```

2. **Execute updates using default sampling**:

```sql
EXEC dbo.usp_UpdateTargetedStatistics
    @DatabaseName   = 'MyDatabase',
    @ExecuteUpdates = 1;
```

3. **Execute updates with `FULLSCAN` for all statistics**:

```sql
EXEC dbo.usp_UpdateTargetedStatistics
    @DatabaseName   = 'MyDatabase',
    @ExecuteUpdates = 1,
    @UseFullScan    = 1,
    @Granularity    = 'STAT';
```

The procedure returns a row for each candidate object showing the command run (or to be run) and whether execution occurred.

## Installation

Run `stored-procedures/usp_UpdateTargetedStatistics.sql` in your SQL Server instance to create the procedure in the `dbo` schema.
