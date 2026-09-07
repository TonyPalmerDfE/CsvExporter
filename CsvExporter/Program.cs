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
            { "Ref_EstablishmentFamily", @"
                    INSERT INTO ref.establishment_family (code, name)
                    SELECT DISTINCT 
                        establishmenttypegroup_code, 
                        COALESCE(establishmenttypegroup_name, 'NOT IN EXTRACT') 
                    FROM staging_table 
                    WHERE establishmenttypegroup_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_EstablishmentType", @"
                    INSERT INTO ref.establishment_type (establishment_family_id, code, name)
                    SELECT DISTINCT 
                        f.establishment_family_id, 
                        s.typeofestablishment_code, 
                        COALESCE(s.typeofestablishment_name, 'NOT IN EXTRACT')
                    FROM staging_table s
                    JOIN ref.establishment_family f ON f.code = s.establishmenttypegroup_code
                    WHERE s.typeofestablishment_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_EstablishmentStatus", @"
                    INSERT INTO ref.establishment_status (code, name)
                    SELECT DISTINCT 
                        establishmentstatus_code, 
                        COALESCE(establishmentstatus_name, 'NOT IN EXTRACT')
                    FROM staging_table 
                    WHERE establishmentstatus_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_Title", @"
                    INSERT INTO ref.title (name)
                    SELECT DISTINCT headtitle_name 
                    FROM staging_table 
                    WHERE headtitle_name IS NOT NULL
                    ON CONFLICT (name) DO NOTHING;" },

            { "Ref_EducationPhaseGroup", @"
                    INSERT INTO ref.education_phase_group (code, name) 
                    VALUES ('ALL', 'All Phases') 
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_EducationPhase", @"
                    INSERT INTO ref.education_phase (education_phase_group_id, code, name)
                    SELECT DISTINCT 
                        pg.education_phase_group_id, 
                        s.phaseofeducation_code, 
                        COALESCE(s.phaseofeducation_name, 'NOT IN EXTRACT')
                    FROM staging_table s
                    CROSS JOIN ref.education_phase_group pg
                    WHERE s.phaseofeducation_code IS NOT NULL AND pg.code = 'ALL'
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_ReasonEstablishmentOpened", @"
                    INSERT INTO ref.reason_establishment_opened (code, name)
                    SELECT DISTINCT 
                        reasonestablishmentopened_code, 
                        COALESCE(reasonestablishmentopened_name, 'NOT IN EXTRACT')
                    FROM staging_table
                    WHERE reasonestablishmentopened_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_ReasonEstablishmentClosed", @"
                    INSERT INTO ref.reason_establishment_closed (code, name)
                    SELECT DISTINCT 
                        reasonestablishmentclosed_code, 
                        COALESCE(reasonestablishmentclosed_name, 'NOT IN EXTRACT')
                    FROM staging_table
                    WHERE reasonestablishmentclosed_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Ref_RoleType", @"
                    INSERT INTO ref.role_type (code, name)
                    SELECT DISTINCT 'HT', 'Headteacher'
                    WHERE NOT EXISTS (SELECT 1 FROM ref.role_type WHERE code = 'HT');" },

            { "Ref_GroupType", @"
                    INSERT INTO ref.group_type (code, name)
                    VALUES 
                        ('TRUST', 'Multi-Academy Trust'),
                        ('FED', 'Federation')
                    ON CONFLICT (code) DO NOTHING;" },

            { "Core_Establishment", @"
                    INSERT INTO core.establishment (urn, uid, name, establishment_number, laestab, dfe_number, establishment_type_id, establishment_status_id)
                    SELECT DISTINCT 
                        s.urn,
                        NULL, 
                        s.establishmentname,
                        s.establishmentnumber,
                        CONCAT(
                            TRIM(s.la_code), 
                            LPAD(TRIM(s.establishmentnumber), 4, '0')
                        ) AS laestab,
                        CONCAT(
                            TRIM(s.la_code), 
                            '/', 
                            LPAD(TRIM(s.establishmentnumber), 4, '0')
                        ) AS dfe_number,
                        t.establishment_type_id,
                        st.establishment_status_id
                    FROM staging_table s
                    JOIN ref.establishment_type t ON t.code = s.typeofestablishment_code
                    JOIN ref.establishment_status st ON st.code = s.establishmentstatus_code
                    WHERE s.urn IS NOT NULL
                    ON CONFLICT (urn) DO NOTHING;" },

            { "Core_EstablishmentAuthority", @"
                    INSERT INTO core.establishment_authority (establishment_id, authority_code, authority_name)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        s.la_code, 
                        s.la_name
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.la_code IS NOT NULL;" },

            { "Core_EstablishmentReligion", @"
                    INSERT INTO core.establishment_religion (establishment_id, religious_character, religious_ethos)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        s.religiouscharacter_name, 
                        s.religiousethos_name
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.religiouscharacter_name IS NOT NULL OR s.religiousethos_name IS NOT NULL;" },

            { "Core_EstablishmentInspection", @"
                    INSERT INTO core.establishment_inspection (establishment_id, inspection_body, inspection_date, inspection_outcome)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        s.inspectoratename_name, 
                        CASE 
                            WHEN TRIM(s.dateoflastinspectionvisit) ~ '^[0-9]{1,2}[^0-9a-zA-Z][0-9]{1,2}[^0-9a-zA-Z][0-9]{2,4}' 
                            THEN TO_DATE(LEFT(REPLACE(REPLACE(TRIM(s.dateoflastinspectionvisit), '/', '-'), '.', '-'), 10), 'DD-MM-YYYY')
                            ELSE NULL 
                        END,
                        s.inspectoratereport
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.inspectoratename_name IS NOT NULL 
                       OR s.dateoflastinspectionvisit IS NOT NULL 
                       OR s.inspectoratereport IS NOT NULL;" },

            { "Core_EstablishmentProvision", @"
                    INSERT INTO core.establishment_provision (establishment_id, fsm, percentage_fsm)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        CASE 
                            WHEN s.fsm ~ '^[0-9]+$' THEN CAST(s.fsm AS INTEGER) 
                            ELSE NULL 
                        END,
                        CASE 
                            WHEN s.percentagefsm ~ '^[0-9\.]+$' THEN CAST(s.percentagefsm AS NUMERIC) 
                            ELSE NULL 
                        END
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    ON CONFLICT (establishment_id) DO NOTHING;" },

            { "Core_EstablishmentIdentifier_UKPRN", @"
                    INSERT INTO core.establishment_identifier (establishment_id, identifier_type, identifier_value)
                    SELECT DISTINCT e.establishment_id, 'UKPRN', s.ukprn
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.ukprn IS NOT NULL
                    ON CONFLICT (establishment_id, identifier_type, identifier_value) DO NOTHING;" },

            { "Core_EstablishmentIdentifier_UPRN", @"
                    INSERT INTO core.establishment_identifier (establishment_id, identifier_type, identifier_value)
                    SELECT DISTINCT e.establishment_id, 'UPRN', s.uprn
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.uprn IS NOT NULL
                    ON CONFLICT (establishment_id, identifier_type, identifier_value) DO NOTHING;" },

            { "Core_Site", @"
                    INSERT INTO core.site (establishment_id, name, address_line_1, address_line_2, town, county, postcode)
                    SELECT DISTINCT 
                        e.establishment_id,
                        s.sitename,
                        s.street,
                        s.locality,
                        s.town,
                        s.county_name,
                        s.postcode
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.street IS NOT NULL;" },

            { "Core_Contact", @"
                    INSERT INTO core.contact (establishment_id, website, telephone_number)
                    SELECT DISTINCT 
                        e.establishment_id,
                        s.schoolwebsite,
                        s.telephonenum
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.schoolwebsite IS NOT NULL OR s.telephonenum IS NOT NULL;" },

            { "Core_EstablishmentAdmissions", @"
                    INSERT INTO core.establishment_admissions (establishment_id, admissions_policy, statutory_low_age, statutory_high_age)
                    SELECT DISTINCT
                        e.establishment_id,
                        s.admissionspolicy_name,
                        CAST(NULLIF(s.statutorylowage, '') AS INTEGER),
                        CAST(NULLIF(s.statutoryhighage, '') AS INTEGER)
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.statutorylowage IS NOT NULL AND s.statutorylowage ~ '^[0-9]+$'
                    ON CONFLICT (establishment_id) DO NOTHING;" },

            { "Core_EstablishmentBoarding", @"
                    INSERT INTO core.establishment_boarding (establishment_id, boarding_provision)
                    SELECT DISTINCT
                        e.establishment_id,
                        s.boarders_name
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.boarders_name IS NOT NULL
                    ON CONFLICT (establishment_id) DO NOTHING;" },

            { "Core_GroupRecord_Trusts", @"
                    INSERT INTO core.group_record (code, name, group_type_id)
                    SELECT DISTINCT 
                        s.trusts_code, 
                        s.trusts_name, 
                        gt.group_type_id
                    FROM staging_table s
                    JOIN ref.group_type gt ON gt.code = 'TRUST'
                    WHERE s.trusts_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Core_GroupRecord_Federations", @"
                    INSERT INTO core.group_record (code, name, group_type_id)
                    SELECT DISTINCT 
                        s.federations_code, 
                        s.federations_name, 
                        gt.group_type_id
                    FROM staging_table s
                    JOIN ref.group_type gt ON gt.code = 'FED'
                    WHERE s.federations_code IS NOT NULL
                    ON CONFLICT (code) DO NOTHING;" },

            { "Core_EstablishmentGroupMembership", @"
                    INSERT INTO core.establishment_group_membership (establishment_id, group_id, membership_category)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        g.group_id, 
                        'Trust Membership'
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    JOIN core.group_record g ON g.code = s.trusts_code
                    WHERE s.trusts_code IS NOT NULL;" },

            { "Core_Person_Headteachers", @"
                    INSERT INTO core.person (title_id, given_name, family_name, display_name)
                    SELECT DISTINCT 
                        t.title_id, 
                        s.headfirstname, 
                        s.headlastname, 
                        TRIM(CONCAT_WS(' ', s.headfirstname, s.headlastname))
                    FROM staging_table s
                    LEFT JOIN ref.title t ON t.name = s.headtitle_name
                    WHERE s.headfirstname IS NOT NULL OR s.headlastname IS NOT NULL;" },

            { "Core_EstablishmentGroupMembership_Federations", @"
                    INSERT INTO core.establishment_group_membership (establishment_id, group_id, membership_category)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        g.group_id, 
                        'Federation Membership'
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    JOIN core.group_record g ON g.code = s.federations_code
                    WHERE s.federations_code IS NOT NULL;" },

            { "Core_Role", @"
                    INSERT INTO core.role (person_id, role_type_id)
                    SELECT DISTINCT p.person_id, rt.role_type_id
                    FROM staging_table s
                    JOIN core.person p ON p.given_name = s.headfirstname AND p.family_name = s.headlastname
                    CROSS JOIN (SELECT role_type_id FROM ref.role_type WHERE code = 'HT' LIMIT 1) rt
                    WHERE s.headfirstname IS NOT NULL OR s.headlastname IS NOT NULL;" },

            { "Core_RoleAssignment", @"
                    INSERT INTO core.role_assignment (role_id, establishment_id, preferred_job_title)
                    SELECT DISTINCT r.role_id, e.establishment_id, s.headpreferredjobtitle
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    JOIN core.person p ON p.given_name = s.headfirstname AND p.family_name = s.headlastname
                    JOIN core.role r ON r.person_id = p.person_id
                    WHERE s.headfirstname IS NOT NULL OR s.headlastname IS NOT NULL;" },

            { "Core_Establishment_UpdateHeadteacher", @"
                    UPDATE core.establishment
                    SET headteacher_role_assignment_id = ra.role_assignment_id
                    FROM core.role_assignment ra
                    WHERE core.establishment.establishment_id = ra.establishment_id;" },

            { "Core_EstablishmentLifecycle_Opened", @"
                    INSERT INTO core.establishment_lifecycle_event (establishment_id, event_type, opened_reason_id, event_date)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        'Opened', 
                        ro.reason_establishment_opened_id,
                        TO_DATE(LEFT(REPLACE(REPLACE(TRIM(s.opendate), '/', '-'), '.', '-'), 10), 'DD-MM-YYYY')
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    LEFT JOIN ref.reason_establishment_opened ro ON ro.code = s.reasonestablishmentopened_code
                    WHERE TRIM(s.opendate) ~ '^[0-9]{1,2}[^0-9a-zA-Z][0-9]{1,2}[^0-9a-zA-Z][0-9]{2,4}';" },

            { "Core_EstablishmentLifecycle_Closed", @"
                    INSERT INTO core.establishment_lifecycle_event (establishment_id, event_type, closed_reason_id, event_date)
                    SELECT DISTINCT 
                        e.establishment_id, 
                        'Closed', 
                        rc.reason_establishment_closed_id,
                        TO_DATE(LEFT(REPLACE(REPLACE(TRIM(s.closedate), '/', '-'), '.', '-'), 10), 'DD-MM-YYYY')
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    LEFT JOIN ref.reason_establishment_closed rc ON rc.code = s.reasonestablishmentclosed_code
                    WHERE TRIM(s.closedate) ~ '^[0-9]{1,2}[^0-9a-zA-Z][0-9]{1,2}[^0-9a-zA-Z][0-9]{2,4}';" },

            { "Core_EstablishmentIdentifier_FEHE", @"
                    INSERT INTO core.establishment_identifier (establishment_id, identifier_type, identifier_value)
                    SELECT DISTINCT e.establishment_id, 'FEHE', s.feheidentifier
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.feheidentifier IS NOT NULL AND TRIM(s.feheidentifier) != ''
                    ON CONFLICT (establishment_id, identifier_type, identifier_value) DO NOTHING;" },

            { "Core_EstablishmentIdentifier_CHNumber", @"
                    INSERT INTO core.establishment_identifier (establishment_id, identifier_type, identifier_value)
                    SELECT DISTINCT e.establishment_id, 'CompaniesHouse', s.chnumber
                    FROM staging_table s
                    JOIN core.establishment e ON e.urn = s.urn
                    WHERE s.chnumber IS NOT NULL AND TRIM(s.chnumber) != ''
                    ON CONFLICT (establishment_id, identifier_type, identifier_value) DO NOTHING;" },

            { "Core_GroupIdentifier_Seeding", @"
                    INSERT INTO core.group_identifier (group_id, identifier_type, identifier_value)
                    SELECT DISTINCT group_id, 'UID', code
                    FROM core.group_record
                    WHERE code IS NOT NULL
                    ON CONFLICT (group_id, identifier_type, identifier_value) DO NOTHING;" },

            { "Core_SearchProvider", @"
                INSERT INTO core.search_provider
                (
                    urn,
                    group_uid,
                    ukprn,
                    laestab,
                    provider_type,
                    address,
                    name,
                    company_house_number,
                    number_of_academies,
                    postcode,
                    county,
                    town,
                    local_authority
                )
                SELECT DISTINCT
                    s.urn,
                    s.trusts_code,
                    s.ukprn,
                    CONCAT(TRIM(s.la_code), LPAD(TRIM(s.establishmentnumber), 4, '0')),
                    s.typeofestablishment_name,
                    CONCAT_WS(', ',
                        s.street,
                        s.locality,
                        s.town,
                        s.county_name,
                        s.postcode
                    ),
                    s.establishmentname,
                    s.chnumber,
                    academy_counts.number_of_academies,
                    s.postcode,
                    s.county_name,
                    s.town,
                    s.la_name
                FROM staging_table s
                LEFT JOIN (
                    SELECT
                        trusts_code,
                        COUNT(DISTINCT urn) AS number_of_academies
                    FROM staging_table
                    WHERE trusts_code IS NOT NULL
                    GROUP BY trusts_code
                ) academy_counts
                    ON academy_counts.trusts_code = s.trusts_code;
                " }

            //{ "Cleanup_Staging", @"DROP TABLE IF EXISTS staging_table;" }
        };
}