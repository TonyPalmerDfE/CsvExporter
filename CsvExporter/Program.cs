using CsvHelper;
using CsvHelper.Configuration;
using Npgsql;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace CsvExporter;

class Program
{
    private const string Host = "localhost";
    private const int Port = 5432;
    private const string Username = "postgres";
    private const string Password = "postgres";
    private const string DatabaseName = "eprdat_development";

    private static int? MaxRows = null;
    private const string CsvFilePath = @"data/edubasealldata.csv";
    private const string SchemaFilePath = @"data/schema.sql";

    private static readonly string AdminConnectionString =
        $"Host={Host};Port={Port};Username={Username};Password={Password};Database=postgres";
    private static readonly string TargetConnectionString =
        $"Host={Host};Port={Port};Username={Username};Password={Password};Database={DatabaseName}";


    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Edubase ELT Pipeline...");

        if (!File.Exists(CsvFilePath))
        {
            Console.WriteLine($"Error: CSV File '{CsvFilePath}' not found.");
            return;
        }

        if (!File.Exists(SchemaFilePath))
        {
            Console.WriteLine($"Error: Schema File '{SchemaFilePath}' not found.");
            return;
        }

        try
        {
            // STEP A: Ensure Database exists, WIPE it clean, and recreate Schema
            await EnsureDatabaseAndSchemaExist();

            // STEP B: Run the ELT Pipeline
            await using var conn = new NpgsqlConnection(TargetConnectionString);
            await conn.OpenAsync();

            Console.WriteLine("Reading CSV headers and generating Staging Table...");
            var headers = GetSanitizedHeaders(CsvFilePath);
            await RecreateStagingTable(conn, headers);

            Console.WriteLine("Streaming data via NpgsqlBinaryImporter (Bulk Copy)...");
            await BulkLoadCsvToStaging(conn, CsvFilePath, headers);

            Console.WriteLine("Executing SQL Mappings to normalized tables...");
            await ExecuteMappingScripts(conn);

            Console.WriteLine("Pipeline completed successfully! Database is seeded.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FATAL ERROR: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
    }

    static async Task EnsureDatabaseAndSchemaExist()
    {
        Console.WriteLine($"Checking if target database '{DatabaseName}' exists...");

        await using (var adminConn = new NpgsqlConnection(AdminConnectionString))
        {
            await adminConn.OpenAsync();

            var checkCmd = new NpgsqlCommand(
                "SELECT 1 FROM pg_database WHERE datname = @dbName",
                adminConn);

            checkCmd.Parameters.AddWithValue("dbName", DatabaseName);

            var exists = await checkCmd.ExecuteScalarAsync() != null;

            if (!exists)
            {
                Console.WriteLine($"Database '{DatabaseName}' not found. Creating it now...");

                var createCmd = new NpgsqlCommand(
                    $"CREATE DATABASE \"{DatabaseName}\"",
                    adminConn);

                await createCmd.ExecuteNonQueryAsync();
            }
        }

        Console.WriteLine("Applying schema.sql for a fresh start...");

        await using (var targetConn = new NpgsqlConnection(TargetConnectionString))
        {
            await targetConn.OpenAsync();

            var wipeSql = @"
                DROP SCHEMA IF EXISTS ref CASCADE;
                DROP SCHEMA IF EXISTS core CASCADE;
                DROP TABLE IF EXISTS staging_table;
                ";

            await using var wipeCmd = new NpgsqlCommand(wipeSql, targetConn);
            await wipeCmd.ExecuteNonQueryAsync();

            string schemaSql = await File.ReadAllTextAsync(SchemaFilePath);
            await using var schemaCmd = new NpgsqlCommand(schemaSql, targetConn);
            await schemaCmd.ExecuteNonQueryAsync();

            Console.WriteLine("Database completely cleared and Schema applied successfully.");
        }
    }

    static string[] GetSanitizedHeaders(string filePath)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true };
        using var reader = new StreamReader(filePath, Encoding.GetEncoding("latin1"));
        using var csv = new CsvReader(reader, config);

        csv.Read();
        csv.ReadHeader();

        return csv.HeaderRecord
            .Select(h => Regex.Replace(h, @"[^a-zA-Z0-9]+", "_").Trim('_').ToLower())
            .ToArray();
    }

    static async Task RecreateStagingTable(NpgsqlConnection conn, string[] headers)
    {
        var createTableSql = new StringBuilder();
        createTableSql.AppendLine("DROP TABLE IF EXISTS staging_table;");
        createTableSql.AppendLine("CREATE TABLE staging_table (");

        var columnDefinitions = headers.Select(h => $"\"{h}\" TEXT").ToList();
        createTableSql.AppendLine(string.Join(",\n    ", columnDefinitions));
        createTableSql.AppendLine(");");

        await using var cmd = new NpgsqlCommand(createTableSql.ToString(), conn);
        await cmd.ExecuteNonQueryAsync();
    }

    static async Task BulkLoadCsvToStaging(NpgsqlConnection conn, string filePath, string[] headers)
    {
        var columns = string.Join(", ", headers.Select(h => $"\"{h}\""));
        string copyCmd = $"COPY staging_table ({columns}) FROM STDIN (FORMAT BINARY)";

        var config = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true };
        using var reader = new StreamReader(filePath, Encoding.GetEncoding("latin1"));
        using var csv = new CsvReader(reader, config);

        await csv.ReadAsync();
        csv.ReadHeader();

        await using var writer = await conn.BeginBinaryImportAsync(copyCmd);

        Console.WriteLine(
            MaxRows.HasValue
                ? $"Loading up to {MaxRows:N0} rows from CSV..."
                : "Loading all rows from CSV...");

        int rowCount = 0;
        while (await csv.ReadAsync())
        {
            if (MaxRows.HasValue && rowCount >= MaxRows.Value)
                break;

            await writer.StartRowAsync();

            for (int i = 0; i < headers.Length; i++)
            {
                var value = csv.GetField(i);

                if (string.IsNullOrWhiteSpace(value))
                    await writer.WriteNullAsync();
                else
                    await writer.WriteAsync(value);
            }

            rowCount++;
        }

        await writer.CompleteAsync();
        Console.WriteLine($"Successfully loaded {rowCount} rows into staging.");
    }

    static async Task ExecuteMappingScripts(NpgsqlConnection conn)
    {
        await using var transaction = await conn.BeginTransactionAsync();
        try
        {
            foreach (var (scriptName, sqlQuery) in MappingScripts)
            {
                Console.WriteLine($" -> Running {scriptName}...");
                await using var cmd = new NpgsqlCommand(sqlQuery, conn, transaction);
                await cmd.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
        }
        catch (Exception)
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    private static readonly Dictionary<string, string> MappingScripts = new()
    {
        {
            "Core_GroupAggregate",
            @"
                INSERT INTO core.group_aggregate
                (
                    group_id,
                    group_uid,
                    name,
 
                    ukprn,
                    companies_house_number,
 
                    site_name,
                    address_line_1,
                    address_line_2,
                    town,
                    county,
                    postcode,
 
                    group_type_name,
 
                    group_status_label,
                    group_status_effective_date
                )
                SELECT
                    group_code,
                    ROW_NUMBER() OVER (ORDER BY group_code),
                    group_name,

                    NULL AS ukprn,
                    NULL AS companies_house_number,

                    NULL AS site_name,
                    NULL AS address_line_1,
                    NULL AS address_line_2,
                    NULL AS town,
                    NULL AS county,
                    NULL AS postcode,

                    group_type_name,

                    NULL AS group_status_label,
                    NULL::DATE AS group_status_effective_date
                FROM
                (
                    SELECT DISTINCT
                        s.trusts_code AS group_code,
                        s.trusts_name AS group_name,
                        'Multi-Academy Trust' AS group_type_name
                    FROM staging_table s
                    WHERE s.trusts_code IS NOT NULL

                    UNION

                    SELECT DISTINCT
                        s.federations_code,
                        s.federations_name,
                        'Federation'
                    FROM staging_table s
                    WHERE s.federations_code IS NOT NULL
                ) groups;
            "
        },
        {
            "Core_EstablishmentAggregate", 
            @"
                INSERT INTO core.establishment_aggregate
                (
                    urn,
                    name,
                    establishment_number,

                    status_name,

                    establishment_type_name,

                    education_phase_name,

                    opened_date,
                    opened_reason,

                    closed_date,
                    closed_reason,

                    group_code,
                    group_uid,
                    group_name,
                    group_type_name,

                    site_name,
                    address_line_1,
                    address_line_2,
                    town,
                    county,
                    postcode,

                    local_authority_code,
                    local_authority_name,

                    statutory_low_age,
                    statutory_high_age,

                    religious_character,

                    ofsted_inspection_date,
                    ofsted_report_url,

                    headteacher_name,

                    website,
                    telephone_number
                )
                SELECT DISTINCT
                    s.urn,
                    s.establishmentname,
                    s.establishmentnumber,

                    s.establishmentstatus_name,

                    s.typeofestablishment_name,

                    s.phaseofeducation_name,

                    CASE
                        WHEN TRIM(s.opendate) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.opendate), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

                    s.reasonestablishmentopened_name,

                    CASE
                        WHEN TRIM(s.closedate) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.closedate), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

                    s.reasonestablishmentclosed_name,

                    COALESCE(
                        s.trusts_code,
                        s.federations_code
                    ),

                    ga.group_uid,

                    COALESCE(
                        s.trusts_name,
                        s.federations_name
                    ),

                    CASE
                        WHEN s.trusts_code IS NOT NULL
                            THEN 'Multi-Academy Trust'
                        WHEN s.federations_code IS NOT NULL
                            THEN 'Federation'
                        ELSE NULL
                    END,

                    s.sitename,
                    s.street,
                    s.locality,
                    s.town,
                    s.county_name,
                    s.postcode,

                    s.la_code,
                    s.la_name,

                    CAST(NULLIF(s.statutorylowage, '') AS INTEGER),
                    CAST(NULLIF(s.statutoryhighage, '') AS INTEGER),

                    s.religiouscharacter_name,

                    CASE
                        WHEN TRIM(s.dateoflastinspectionvisit) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.dateoflastinspectionvisit), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

                    s.inspectoratereport,

                    TRIM(CONCAT_WS(
                        ' ',
                        s.headfirstname,
                        s.headlastname)),

                    s.schoolwebsite,
                    s.telephonenum

                FROM staging_table s
                LEFT JOIN core.group_aggregate ga
                    ON ga.group_id = COALESCE(
                        s.trusts_code,
                        s.federations_code
                    )
                WHERE s.urn IS NOT NULL;
            "
        },
        { 
            "Core_EstablishmentGovernorAggregate",
            @"
                INSERT INTO core.establishment_governor_aggregate
                (
                    establishment_urn,
                    governor_id,
                    governor_name,
                    start_date
                )
                SELECT DISTINCT
                    s.urn,
                    md5(
                        COALESCE(s.headfirstname, '')
                        || COALESCE(s.headlastname, '')
                    ),
                    TRIM(CONCAT_WS(
                        ' ',
                        s.headfirstname,
                        s.headlastname)),
                    NULL::DATE
                FROM staging_table s
                WHERE s.headfirstname IS NOT NULL
                OR s.headlastname IS NOT NULL;
            "},
        {
            "Core_GroupMemberAggregate",
            @"
                INSERT INTO core.group_member_aggregate
                (
                    group_id,
                    establishment_urn,
                    establishment_name
                )
                SELECT DISTINCT
                    s.trusts_code,
                    s.urn,
                    s.establishmentname
                FROM staging_table s
                WHERE s.trusts_code IS NOT NULL

                UNION

                SELECT DISTINCT
                    s.federations_code,
                    s.urn,
                    s.establishmentname
                FROM staging_table s
                WHERE s.federations_code IS NOT NULL;
            "
        },
        {
            "Core_SearchAggregate",
            @"
                INSERT INTO core.search_aggregate
                (
                    provider_id,
                    provider_name,
                    la_estab,
                    dfe_number,
                    provider_type_name,
                    provider_type_id,
                    provider_address,
                    companies_house_number,
                    uk_provider_reference_number,
                    postcode,
                    county,
                    town,
                    local_authority_name,
                    group_id,
                    group_uid,
                    academy_counts,
                    provider_category
                )
                -- Establishments
                SELECT DISTINCT
                    ea.urn AS provider_id,
                    ea.name AS provider_name,

                    NULL AS la_estab,
                    NULL AS dfe_number,

                    ea.establishment_type_name AS provider_type_name,
                    NULL::BIGINT AS provider_type_id,

                    CONCAT_WS(', ',
                        ea.address_line_1,
                        ea.address_line_2,
                        ea.town,
                        ea.county,
                        ea.postcode
                    ) AS provider_address,

                    NULL AS companies_house_number,
                    NULL AS uk_provider_reference_number,

                    ea.postcode,
                    ea.county,
                    ea.town,

                    ea.local_authority_name,

                    ea.group_code AS group_id,
                    ea.group_uid::text AS group_uid,

                    0 AS academy_counts,

                    'Establishment' AS provider_category

                FROM core.establishment_aggregate ea

                UNION ALL

                -- Groups
                SELECT DISTINCT
                    ga.group_id AS provider_id,
                    ga.name AS provider_name,

                    NULL AS la_estab,
                    NULL AS dfe_number,

                    ga.group_type_name AS provider_type_name,
                    NULL::BIGINT AS provider_type_id,

                    NULL AS provider_address,

                    NULL AS companies_house_number,
                    NULL AS uk_provider_reference_number,

                    NULL AS postcode,
                    NULL AS county,
                    NULL AS town,
                    NULL AS local_authority_name,

                    ga.group_id,
                    ga.group_uid::text,

                    COALESCE(member_counts.academy_count, 0),

                    'Group' AS provider_category

                FROM core.group_aggregate ga

                LEFT JOIN
                (
                    SELECT
                        group_id,
                        COUNT(*) AS academy_count
                    FROM core.group_member_aggregate
                    GROUP BY group_id
                ) member_counts
                    ON member_counts.group_id = ga.group_id;
            "},
            //{ "Cleanup_Staging", @"DROP TABLE IF EXISTS staging_table;" }
        };
}
