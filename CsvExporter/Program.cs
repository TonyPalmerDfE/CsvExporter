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
    private const string EstablishmentsCsvFilePath = @"data/edubaseallestablishments.csv";
    private const string GroupsCsvFilePath = @"data/edubaseallgroups.csv";
    private const string SchemaFilePath = @"data/schema.sql";

    private static readonly string AdminConnectionString =
        $"Host={Host};Port={Port};Username={Username};Password={Password};Database=postgres";
    private static readonly string TargetConnectionString =
        $"Host={Host};Port={Port};Username={Username};Password={Password};Database={DatabaseName}";


    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Edubase ELT Pipeline...");

        if (!File.Exists(EstablishmentsCsvFilePath))
        {
            Console.WriteLine($"Error: CSV File '{EstablishmentsCsvFilePath}' not found.");
            return;
        }

        if (!File.Exists(GroupsCsvFilePath))
        {
            Console.WriteLine(
                $"Error: Groups CSV file '{GroupsCsvFilePath}' not found.");
            return;
        }

        if (!File.Exists(SchemaFilePath))
        {
            Console.WriteLine($"Error: Schema File '{SchemaFilePath}' not found.");
            return;
        }

        try
        {
            // STEP A: Ensure database exists, wipe it clean, and recreate schema
            await EnsureDatabaseAndSchemaExist();

            // STEP B: Run the ELT pipeline
            await using NpgsqlConnection conn =
                new(TargetConnectionString);

            await conn.OpenAsync();

            Console.WriteLine(
                "Reading establishments CSV headers and generating staging table...");

            string[] establishmentHeaders =
                GetSanitizedHeaders(EstablishmentsCsvFilePath);

            await RecreateStagingTable(
                conn,
                "staging_establishments",
                establishmentHeaders);

            Console.WriteLine(
                "Streaming establishments via NpgsqlBinaryImporter...");

            await BulkLoadCsvToStaging(
                conn,
                "staging_establishments",
                EstablishmentsCsvFilePath,
                establishmentHeaders);

            Console.WriteLine(
                "Reading groups CSV headers and generating staging table...");

            string[] groupHeaders =
                GetSanitizedHeaders(GroupsCsvFilePath);

            await RecreateStagingTable(
                conn,
                "staging_groups",
                groupHeaders);

            Console.WriteLine(
                "Streaming groups via NpgsqlBinaryImporter...");

            await BulkLoadCsvToStaging(
                conn,
                "staging_groups",
                GroupsCsvFilePath,
                groupHeaders);

            Console.WriteLine("Executing SQL mappings to normalized tables...");
            await ExecuteMappingScripts(conn);

            Console.WriteLine(
                "Pipeline completed successfully! Database is seeded.");
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

            string wipeSql = @"
                DROP SCHEMA IF EXISTS ref CASCADE;
                DROP SCHEMA IF EXISTS core CASCADE;
                DROP TABLE IF EXISTS staging_establishments;
                DROP TABLE IF EXISTS staging_groups;
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

    static async Task RecreateStagingTable(
        NpgsqlConnection conn,
        string tableName,
        string[] headers)
    {
        StringBuilder createTableSql = new();

        createTableSql.AppendLine($"DROP TABLE IF EXISTS \"{tableName}\";");
        createTableSql.AppendLine($"CREATE TABLE \"{tableName}\" (");

        List<string> columnDefinitions = [.. headers.Select(header => $"\"{header}\" TEXT")];

        createTableSql.AppendLine(
            string.Join(",\n    ", columnDefinitions));

        createTableSql.AppendLine(");");

        await using NpgsqlCommand cmd =
            new(createTableSql.ToString(), conn);

        await cmd.ExecuteNonQueryAsync();
    }

    static async Task BulkLoadCsvToStaging(
        NpgsqlConnection conn,
        string tableName,
        string filePath,
        string[] headers)
    {
        string columns =
            string.Join(
                ", ",
                headers.Select(header => $"\"{header}\""));

        string copyCommand =
            $"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)";

        CsvConfiguration config = new(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true
        };

        using StreamReader reader =
            new(filePath, Encoding.GetEncoding("latin1"));

        using CsvReader csv =
            new(reader, config);

        await csv.ReadAsync();
        csv.ReadHeader();

        await using NpgsqlBinaryImporter writer =
            await conn.BeginBinaryImportAsync(copyCommand);

        Console.WriteLine(
            MaxRows.HasValue
                ? $"Loading up to {MaxRows:N0} rows from '{filePath}'..."
                : $"Loading all rows from '{filePath}'...");

        int rowCount = 0;

        while (await csv.ReadAsync())
        {
            if (MaxRows.HasValue && rowCount >= MaxRows.Value)
            {
                break;
            }

            await writer.StartRowAsync();

            for (int i = 0; i < headers.Length; i++)
            {
                string? value = csv.GetField(i);

                if (string.IsNullOrWhiteSpace(value))
                {
                    await writer.WriteNullAsync();
                }
                else
                {
                    await writer.WriteAsync(value);
                }
            }

            rowCount++;
        }

        await writer.CompleteAsync();

        Console.WriteLine(
            $"Successfully loaded {rowCount} rows into '{tableName}'.");
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
                group_status_effective_date,

                opened_date,
                closed_date
            )
            SELECT
                NULLIF(TRIM(s.group_id), ''),

                NULLIF(TRIM(s.group_uid), '')::BIGINT,

                NULLIF(TRIM(s.ukprn), ''),

                NULLIF(
                    TRIM(s.companies_house_number),
                    ''
                ),

                NULLIF(TRIM(s.group_name), ''),

                NULLIF(TRIM(s.group_type), ''),

                NULL AS headteacher_identifier,

                NULLIF(
                    TRIM(
                        CONCAT_WS(
                            ' ',
                            NULLIF(
                                TRIM(s.head_of_group_first_name),
                                ''
                            ),
                            NULLIF(
                                TRIM(s.head_of_group_last_name),
                                ''
                            )
                        )
                    ),
                    ''
                ) AS headteacher_name,

                NULL AS religious_character,

                NULL AS website,
                NULL AS telephone_number,

                NULL AS site_name,

                NULLIF(
                    TRIM(s.group_street),
                    ''
                ) AS address_line_1,

                NULLIF(
                    CONCAT_WS(
                        ', ',
                        NULLIF(
                            TRIM(s.group_locality),
                            ''
                        ),
                        NULLIF(
                            TRIM(s.group_address_3),
                            ''
                        )
                    ),
                    ''
                ) AS address_line_2,

                NULLIF(
                    TRIM(s.group_town),
                    ''
                ) AS town,

                NULLIF(
                    TRIM(s.group_county),
                    ''
                ) AS county,

                NULLIF(
                    TRIM(s.group_postcode),
                    ''
                ) AS postcode,

                NULLIF(
                    TRIM(s.group_status),
                    ''
                ) AS group_status_label,

                NULL::DATE AS group_status_effective_date,

                CASE
                    WHEN TRIM(s.open_date) ~
                        '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                    THEN TO_DATE(
                        TRIM(s.open_date),
                        'DD/MM/YYYY'
                    )
                    ELSE NULL
                END AS opened_date,

                CASE
                    WHEN TRIM(s.closed_date) ~
                        '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                    THEN TO_DATE(
                        TRIM(s.closed_date),
                        'DD/MM/YYYY'
                    )
                    ELSE NULL
                END AS closed_date

            FROM staging_groups s

            WHERE NULLIF(
                TRIM(s.group_uid),
                ''
            ) IS NOT NULL;
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

                    gender,
                    statutory_low_age,
                    statutory_high_age,

                    religious_character,
                    religious_ethos,
                    diocese,

                    sen_provision,
                    sen,
        
                    number_of_pupils,
                    number_of_boys,
                    number_of_girls,
                    fsm_percentage,
                    fsm_number,

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
                        NULLIF(TRIM(s.trusts_code), ''),
                        NULLIF(TRIM(s.federations_code), '')
                    ) AS group_code,

                    ga.group_uid,

                    ga.name AS group_name,

                    ga.group_type_name,

                    s.sitename,
                    s.street,
                    s.locality,
                    s.town,
                    s.county_name,
                    s.postcode,

                    CAST(NULLIF(s.la_code, '') AS INTEGER),
                    s.la_name,

                    s.gender_name,
                    CAST(NULLIF(s.statutorylowage, '') AS INTEGER),
                    CAST(NULLIF(s.statutoryhighage, '') AS INTEGER),

                    s.religiouscharacter_name,
                    NULLIF(TRIM(s.religiousethos_name), ''),
                    NULLIF(TRIM(s.diocese_name), ''),
                    s.typeofresourcedprovision_name,
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
                    ),
                    NULLIF(TRIM(s.numberofpupils), '')::INTEGER,
                    NULLIF(TRIM(s.numberofboys), '')::INTEGER,
                    NULLIF(TRIM(s.numberofgirls), '')::INTEGER,
                    NULLIF(TRIM(s.percentagefsm), '')::NUMERIC,
                    NULLIF(TRIM(s.fsm), '')::INTEGER,

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

                FROM staging_establishments s

            LEFT JOIN core.group_aggregate ga
                ON ga.group_uid::TEXT = COALESCE(
                    NULLIF(TRIM(s.trusts_code), ''),
                    NULLIF(TRIM(s.federations_code), '')
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
                FROM staging_establishments s
                WHERE s.headfirstname IS NOT NULL
                OR s.headlastname IS NOT NULL;
            "},
        {
            "Core_GroupMemberAggregate",
            @"
                INSERT INTO core.group_member_aggregate
                (
                    group_uid,
                    establishment_urn,
                    establishment_name
                )
                SELECT DISTINCT
                    s.trusts_code::BIGINT,
                    s.urn,
                    s.establishmentname
                FROM staging_establishments s
                WHERE s.trusts_code IS NOT NULL

                UNION

                SELECT DISTINCT
                    s.federations_code::BIGINT,
                    s.urn,
                    s.establishmentname
                FROM staging_establishments s
                WHERE s.federations_code IS NOT NULL;
            "
    },
    {
        "Core_GroupEstablishmentAggregate",
        @"
            INSERT INTO core.group_establishment_aggregate
            (
                group_uid,
                establishment_urn,
                establishment_name
            )
            SELECT DISTINCT
                s.trusts_code::BIGINT,
                s.urn,
                s.establishmentname
            FROM staging_establishments s
            WHERE s.trusts_code IS NOT NULL

            UNION

            SELECT DISTINCT
                s.federations_code::BIGINT,
                s.urn,
                s.establishmentname
            FROM staging_establishments s
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

            COALESCE(
                NULLIF(
                    TRIM(s.furthereducationtype_name), ''),
                    'Not recorded') 
                AS further_education_type,

            COALESCE(
                NULLIF(
                    TRIM(s.admissionspolicy_name), ''),
                    'Not recorded') 
                AS admissions,

            COALESCE(
                NULLIF(
                    TRIM(s.boardingestablishment_name), ''),
                    'Not recorded') 
                AS boarding,

            COALESCE(
                NULLIF(
                    TRIM(s.nurseryprovision_name), ''),
                    'Not recorded') 
                AS nursery,

            COALESCE(
                NULLIF(
                    TRIM(s.gender_name), ''),
                    'Not recorded') 
                AS gender,

            ea.statutory_low_age AS age_low,
            ea.statutory_high_age AS age_high,

            ea.group_name AS group_name,
            ega.group_id AS group_id,
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

            COALESCE(
                NULLIF(
                    TRIM(s.officialsixthform_name), ''),
                    'Not recorded')
                AS sixth_form,

            NULLIF(TRIM(s.section41approved_name), '')
                AS section_41,

            COALESCE(
                NULLIF(
                    TRIM(s.specialclasses_name), ''),
                    'Not recorded') 
                AS special_class

        FROM core.establishment_aggregate ea

        LEFT JOIN staging_establishments s
            ON s.urn = ea.urn

        LEFT JOIN core.group_aggregate ega
            ON ega.group_uid = ea.group_uid

        UNION ALL

        -- Groups
        SELECT DISTINCT
            ga.group_uid::TEXT AS provider_id,
            ga.name AS provider_name,

            NULL::TEXT AS la_estab,
            NULL::TEXT AS dfe_number,

            ga.group_type_name AS provider_type_name,

            NULLIF(
                CONCAT_WS(
                    ', ',
                    NULLIF(TRIM(ga.address_line_1), ''),
                    NULLIF(TRIM(ga.address_line_2), ''),
                    NULLIF(TRIM(ga.town), ''),
                    NULLIF(TRIM(ga.county), ''),
                    NULLIF(TRIM(ga.postcode), '')
                ),
                ''
            ) AS provider_address,

            NULLIF(TRIM(ga.companies_house_number), '')
                AS companies_house_number,

            NULLIF(TRIM(ga.ukprn), '')
                AS uk_provider_reference_number,

            NULLIF(TRIM(ga.postcode), '') AS postcode,
            NULLIF(TRIM(ga.county), '') AS county,
            NULLIF(TRIM(ga.town), '') AS town,

            NULL::TEXT AS local_authority_name,

            NULL::INTEGER AS status_code,

            NULLIF(TRIM(ga.group_status_label), '')
                AS status_name,

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
                establishment_counts.establishment_count,
                0
            ) AS academy_counts,

            ga.opened_date AS opened_date,
            ga.closed_date AS closed_date,

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
                group_uid,
                COUNT(DISTINCT establishment_urn)::INTEGER
                    AS establishment_count
            FROM core.group_establishment_aggregate
            GROUP BY group_uid
        ) establishment_counts
            ON establishment_counts.group_uid = ga.group_uid;
        "
    },
            //{ "Cleanup_Staging", @"DROP TABLE IF EXISTS staging_establishments;" }
            //{ "Cleanup_Staging", @"DROP TABLE IF EXISTS staging_groups;" }
        };
}
