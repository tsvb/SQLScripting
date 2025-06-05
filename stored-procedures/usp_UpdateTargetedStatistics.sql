CREATE OR ALTER PROCEDURE dbo.usp_UpdateTargetedStatistics
(
    @DatabaseName       sysname        = NULL,     -- Target database name (cannot be NULL)
    @MinRowCount        BIGINT         = 100000,   -- Ignore tables smaller than this
    @MinModificationPct DECIMAL(10,4)  = 10.0,     -- % of rows modified threshold
    @ExecuteUpdates     BIT            = 0,        -- 0 = dry-run; 1 = actually run UPDATE STATISTICS
    @UseFullScan        BIT            = 0,        -- 1 = FULLSCAN; 0 = RESAMPLE
    @Granularity        CHAR(5)        = 'TABLE'   -- 'TABLE' or 'STAT'
)
AS
BEGIN
    SET NOCOUNT ON;

    ------------------------------------------------------------------------
    -- 1) Validate
    ------------------------------------------------------------------------
    IF @DatabaseName IS NULL
    BEGIN
        THROW 50001, 'The @DatabaseName parameter must be specified.', 1;
    END

    ------------------------------------------------------------------------
    -- 2) Prepare parameter‐definition string for sp_executesql
    ------------------------------------------------------------------------
    DECLARE @paramDef NVARCHAR(MAX) = 
        N'@MinRowCount BIGINT,
          @MinModificationPct DECIMAL(10,4),
          @ExecuteUpdates BIT,
          @UseFullScan BIT,
          @Granularity CHAR(5)';

    ------------------------------------------------------------------------
    -- 3) Build the dynamic SQL batch in @sql
    --    It will run inside the context of @DatabaseName
    ------------------------------------------------------------------------
    DECLARE @sql NVARCHAR(MAX);

    SET @sql =
        -- Start of quoted literal for the inner batch
        N'
        SET NOCOUNT ON;

        ----------------------------------------------------------------
        -- Gather metadata for candidate statistics
        ----------------------------------------------------------------
        ;WITH CandidateStats AS
        (
            SELECT
                s.name                        AS SchemaName,
                t.name                        AS TableName,
                st.stats_id,
                st.name                       AS StatsName,
                ps.row_count                  AS CurrentRows,
                sp.rows                       AS StatsRows,
                sp.modification_counter       AS Modifications,
                sp.last_updated               AS LastUpdated,
                CAST(
                    CASE 
                        WHEN ps.row_count = 0 THEN 0
                        ELSE (100.0 * sp.modification_counter) / ps.row_count
                    END 
                    AS DECIMAL(19,4)
                )                              AS PercentModified
            FROM   ' + QUOTENAME(@DatabaseName) + N'.sys.schemas     AS s
            JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.tables      AS t ON t.schema_id = s.schema_id
            JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.stats       AS st ON st.object_id = t.object_id
            OUTER APPLY ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
            JOIN 
            (
                SELECT object_id, SUM(row_count) AS row_count
                FROM   ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_partition_stats
                WHERE  index_id IN (0,1)  -- heap or clustered index
                GROUP BY object_id
            ) AS ps ON ps.object_id = t.object_id
            WHERE
                st.name NOT LIKE ''_WA_Sys%''
                AND sp.modification_counter IS NOT NULL
                AND ps.row_count >= @MinRowCount
                AND ((100.0 * sp.modification_counter) / ps.row_count) >= @MinModificationPct
        ),
        TargetObjects AS
        (
            SELECT
                SchemaName,
                TableName,
                CASE 
                    WHEN @Granularity = ''STAT'' THEN QUOTENAME(StatsName) 
                    ELSE NULL 
                END                        AS StatsName,
                MAX(CurrentRows)           AS CurrentRows,
                MAX(PercentModified)       AS PercentModified,
                MAX(LastUpdated)           AS LastUpdated
            FROM CandidateStats
            GROUP BY
                SchemaName,
                TableName,
                CASE WHEN @Granularity = ''STAT'' THEN QUOTENAME(StatsName) ELSE NULL END
        ),
        Commands AS
        (
            SELECT
                SchemaName,
                TableName,
                StatsName,
                CurrentRows,
                PercentModified,
                LastUpdated,
                -- Build the UPDATE STATISTICS statement
                N''UPDATE STATISTICS '' 
                    + QUOTENAME(SchemaName) + N''.'' + QUOTENAME(TableName) 
                    + CASE 
                          WHEN StatsName IS NOT NULL 
                          THEN N'' '' + StatsName 
                          ELSE N'''' 
                      END
                    + N'' WITH '' 
                    + CASE 
                          WHEN @UseFullScan = 1 THEN N''FULLSCAN'' 
                          ELSE N''RESAMPLE'' 
                      END
                    + N'';'' 
                AS UpdateCommand
            FROM TargetObjects
        )

        ----------------------------------------------------------------
        -- Store commands in temp table and optionally execute
        ----------------------------------------------------------------
        SELECT *
        INTO   #ExecList
        FROM   Commands;

        IF @ExecuteUpdates = 1
        BEGIN
            DECLARE cmdCur CURSOR LOCAL FAST_FORWARD FOR
                SELECT UpdateCommand FROM #ExecList;

            DECLARE @cmd NVARCHAR(MAX);
            OPEN cmdCur;
            FETCH NEXT FROM cmdCur INTO @cmd;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    EXEC (@cmd);
                END TRY
                BEGIN CATCH
                    PRINT ''Error executing: '' + @cmd;
                    PRINT ERROR_MESSAGE();
                END CATCH;

                FETCH NEXT FROM cmdCur INTO @cmd;
            END
            CLOSE cmdCur;
            DEALLOCATE cmdCur;
        END

        -- Return audit/result set
        SELECT
            SchemaName,
            TableName,
            StatsName,
            CurrentRows,
            PercentModified,
            LastUpdated,
            UpdateCommand,
            @ExecuteUpdates AS Executed
        FROM #ExecList;
        '  -- End of the inner quoted batch

    -- 4) At this point @sql is: 
    --      N'   <inner batch with doubled quotes>   '
    --    Next we close that literal and append the parameter-definition and assignments.

    SET @sql = @sql
        + N','  -- close the inner batch literal, start parameter-definition literal
        + N'@MinRowCount BIGINT,
          @MinModificationPct DECIMAL(10,4),
          @ExecuteUpdates BIT,
          @UseFullScan BIT,
          @Granularity CHAR(5)'
        -- 5) Now append the parameter assignments (outside the quoted literal)
        + N', @MinRowCount='       + CAST(@MinRowCount        AS NVARCHAR(20))
        + N', @MinModificationPct=' + CAST(@MinModificationPct AS NVARCHAR(20))
        + N', @ExecuteUpdates='     + CAST(@ExecuteUpdates     AS NVARCHAR(5))
        + N', @UseFullScan='        + CAST(@UseFullScan        AS NVARCHAR(5))
        + N', @Granularity='        + QUOTENAME(@Granularity, '''');  -- embed the single quotes around @Granularity

    ------------------------------------------------------------------------
    -- 6) Execute the assembled batch via sp_executesql
    ------------------------------------------------------------------------
    EXEC sp_executesql
        @sql,
        @paramDef,
        @MinRowCount        = @MinRowCount,
        @MinModificationPct = @MinModificationPct,
        @ExecuteUpdates     = @ExecuteUpdates,
        @UseFullScan        = @UseFullScan,
        @Granularity        = @Granularity;
END
GO
