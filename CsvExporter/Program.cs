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
    private const int Port = 5440;
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

                    ukprn,
                    companies_house_number,

                    name,
                    group_type_name,

                    headteacher_identifier,
                    headteacher_name,

                    religious_character,

                    website,
                    telephone_number,

                    site_name,
                    address_line_1,
                    address_line_2,
                    town,
                    county,
                    postcode,

                    group_status_label,
                    group_status_effective_date
                )
                SELECT
                    group_code,

                    ROW_NUMBER() OVER (ORDER BY group_code),

                    NULL AS ukprn,
                    NULL AS companies_house_number,

                    group_name,
                    group_type_name,

                    NULL AS headteacher_identifier,
                    NULL AS headteacher_name,

                    NULL AS religious_character,

                    NULL AS website,
                    NULL AS telephone_number,

                    NULL AS site_name,
                    NULL AS address_line_1,
                    NULL AS address_line_2,
                    NULL AS town,
                    NULL AS county,
                    NULL AS postcode,

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
                    ukprn,
                    name,
                    establishment_number,

                    status_code,
                    status_date,

                    establishment_type_name,

                    education_phase_name,

                    opened_date,
                    closed_date,

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
                    NULLIF(TRIM(s.ukprn), ''),
                    s.establishmentname,
                    s.establishmentnumber,

                    CAST(NULLIF(s.establishmentstatus_code, '') AS INTEGER),

                    CASE
                        WHEN TRIM(s.opendate) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.opendate), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

                    s.typeofestablishment_name,

                    s.phaseofeducation_name,

                    CASE
                        WHEN TRIM(s.opendate) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.opendate), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

                    CASE
                        WHEN TRIM(s.closedate) ~ '^[0-9]'
                        THEN TO_DATE(
                            LEFT(REPLACE(REPLACE(TRIM(s.closedate), '/', '-'), '.', '-'), 10),
                            'DD-MM-YYYY')
                        ELSE NULL
                    END,

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

                    CAST(NULLIF(s.la_code, '') AS INTEGER),
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
                        s.headlastname
                    )),

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
            provider_address,
            companies_house_number,
            uk_provider_reference_number,
            postcode,
            county,
            town,
            local_authority_name,
            status_code,
            status_name,
            education_phase,
            further_education_type,
            admissions,
            boarding,
            nursery,
            gender,
            age_low,
            age_high,
            group_name,
            group_id,
            group_uid,
            group_status,
            group_type,
            academy_counts,
            opened_date,
            closed_date,
            provider_category,
            sen,
            diocese,
            religious_character,
            religious_ethos,
            sixth_form,
            section_41,
            special_class
        )

        -- Establishments
        SELECT DISTINCT
            ea.urn AS provider_id,
            ea.name AS provider_name,

            CASE
                WHEN ea.local_authority_code BETWEEN 0 AND 999
                    AND TRIM(ea.establishment_number) ~ '^[0-9]{1,4}$'
                THEN LPAD(ea.local_authority_code::TEXT, 3, '0')
                    || LPAD(ea.establishment_number, 4, '0')
                ELSE NULL
            END AS la_estab,

            CASE
                WHEN ea.local_authority_code BETWEEN 0 AND 999
                    AND TRIM(ea.establishment_number) ~ '^[0-9]{1,4}$'
                THEN LPAD(ea.local_authority_code::TEXT, 3, '0')
                    || '/'
                    || LPAD(ea.establishment_number, 4, '0')
                ELSE NULL
            END AS dfe_number,

            ea.establishment_type_name AS provider_type_name,

            NULLIF(
                CONCAT_WS(
                    ', ',
                    NULLIF(TRIM(ea.address_line_1), ''),
                    NULLIF(TRIM(ea.address_line_2), ''),
                    NULLIF(TRIM(ea.town), ''),
                    NULLIF(TRIM(ea.county), ''),
                    NULLIF(TRIM(ea.postcode), '')
                ),
                ''
            ) AS provider_address,

            NULLIF(TRIM(s.chnumber), '') AS companies_house_number,
            NULLIF(TRIM(ea.ukprn), '') AS uk_provider_reference_number,

            NULLIF(TRIM(ea.postcode), '') AS postcode,
            NULLIF(TRIM(ea.county), '') AS county,
            NULLIF(TRIM(ea.town), '') AS town,
            NULLIF(TRIM(ea.local_authority_name), '') AS local_authority_name,

            ea.status_code AS status_code,

            NULLIF(TRIM(s.establishmentstatus_name), '') AS status_name,

            NULLIF(TRIM(s.phaseofeducation_name), '') AS education_phase,

            NULLIF(TRIM(s.furthereducationtype_name), '')
                AS further_education_type,

            NULLIF(TRIM(s.admissionspolicy_name), '') AS admissions,

            NULLIF(TRIM(s.boardingestablishment_name), '') AS boarding,

            NULLIF(TRIM(s.nurseryprovision_name), '') AS nursery,

            NULLIF(TRIM(s.gender_name), '') AS gender,

            ea.statutory_low_age AS age_low,
            ea.statutory_high_age AS age_high,

            ea.group_name AS group_name,
            ea.group_code AS group_id,
            ea.group_uid::TEXT AS group_uid,

            NULL::INTEGER AS group_status,

            ea.group_type_name AS group_type,

            0 AS academy_counts,

            ea.opened_date AS opened_date,
            ea.closed_date AS closed_date,

            'Establishment' AS provider_category,

            ARRAY_REMOVE(
                ARRAY[
                    NULLIF(TRIM(s.sen1_name), ''),
                    NULLIF(TRIM(s.sen2_name), ''),
                    NULLIF(TRIM(s.sen3_name), ''),
                    NULLIF(TRIM(s.sen4_name), ''),
                    NULLIF(TRIM(s.sen5_name), ''),
                    NULLIF(TRIM(s.sen6_name), ''),
                    NULLIF(TRIM(s.sen7_name), ''),
                    NULLIF(TRIM(s.sen8_name), ''),
                    NULLIF(TRIM(s.sen9_name), ''),
                    NULLIF(TRIM(s.sen10_name), ''),
                    NULLIF(TRIM(s.sen11_name), ''),
                    NULLIF(TRIM(s.sen12_name), ''),
                    NULLIF(TRIM(s.sen13_name), '')
                ],
                NULL
            ) AS sen,

            NULLIF(TRIM(s.diocese_name), '')
                AS diocese,

            NULLIF(TRIM(s.religiouscharacter_name), '')
                AS religious_character,

            NULLIF(TRIM(s.religiousethos_name), '')
                AS religious_ethos,

            NULLIF(TRIM(s.officialsixthform_name), '')
                AS sixth_form,

            NULLIF(TRIM(s.section41approved_name), '')
                AS section_41,

            NULLIF(TRIM(s.specialclasses_name), '')
                AS special_class

        FROM core.establishment_aggregate ea

        LEFT JOIN staging_table s
            ON s.urn = ea.urn

        UNION ALL

        -- Groups
        SELECT DISTINCT
            ga.group_id AS provider_id,
            ga.name AS provider_name,

            NULL::TEXT AS la_estab,
            NULL::TEXT AS dfe_number,

            ga.group_type_name AS provider_type_name,

            NULL::TEXT AS provider_address,

            NULL::TEXT AS companies_house_number,
            NULL::TEXT AS uk_provider_reference_number,

            NULL::TEXT AS postcode,
            NULL::TEXT AS county,
            NULL::TEXT AS town,
            NULL::TEXT AS local_authority_name,

            NULL::INTEGER AS status_code,
            NULL::TEXT AS status_name,

            NULL::TEXT AS education_phase,
            NULL::TEXT AS further_education_type,
            NULL::TEXT AS admissions,
            NULL::TEXT AS boarding,
            NULL::TEXT AS nursery,
            NULL::TEXT AS gender,

            NULL::INTEGER AS age_low,
            NULL::INTEGER AS age_high,

            ga.name AS group_name,
            ga.group_id AS group_id,
            ga.group_uid::TEXT AS group_uid,

            NULL::INTEGER AS group_status,

            ga.group_type_name AS group_type,

            COALESCE(
                member_counts.academy_count,
                0
            ) AS academy_counts,

            NULL::DATE AS opened_date,
            NULL::DATE AS closed_date,

            'Group' AS provider_category,

            ARRAY[]::TEXT[] AS sen,

            NULL::TEXT AS diocese,
            NULL::TEXT AS religious_character,
            NULL::TEXT AS religious_ethos,
            NULL::TEXT AS sixth_form,
            NULL::TEXT AS section_41,
            NULL::TEXT AS special_class

        FROM core.group_aggregate ga

        LEFT JOIN
        (
            SELECT
                group_id,
                COUNT(*)::INTEGER AS academy_count
            FROM core.group_member_aggregate
            GROUP BY group_id
        ) member_counts
            ON member_counts.group_id = ga.group_id;
    "
},
            //{ "Cleanup_Staging", @"DROP TABLE IF EXISTS staging_table;" }
        };
}
