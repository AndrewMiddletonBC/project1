using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Npgsql;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;

using System.Data;
using System.Xml;
using System.Text.Json;
using Amazon.S3.Model;

/*
 * Code by Andrew
 */


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Project1Lambda;

public class Function
{
    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<string?> FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        S3EventNotification.S3Entity s3Entity = evnt.Records?[0].S3;
        if (s3Entity == null)
        {
            return null;
        }


        Console.WriteLine("Bucket: {0}", s3Entity.Bucket.Name);
        Console.WriteLine("File: {0}", s3Entity.Object.Key);


        // Retrieve file type tag of object added to s3 bucket
        string? s3filetype = GetEventObjectFileType(s3Entity);

        // Retrieve string contents of object added to s3 bucket
        string? s3contents = GetEventObjectContents(s3Entity);

        // handle invalid contents and type tag
        if (s3contents == null)
        {
            Console.WriteLine("File not found");
            return null;
        }
        else if (s3contents.Length == 0)
        {
            Console.WriteLine("File is Empty");
            return null;
        }
        else if (s3filetype == null)
        {
            Console.WriteLine("Type tag not found");
            return null;
        }

        // debug file contents logging
        Console.WriteLine(s3contents);

        // setup db objects
        VaccineData vaccineData = null;

        // process file
        if (s3filetype.Equals("xml"))
        {
            vaccineData = ProcessXml(s3contents);
        }
        else if (s3filetype.Equals("json"))
        {
            JsonSerializerOptions serializerOptions = new JsonSerializerOptions(new JsonSerializerDefaults());
            serializerOptions.Converters.Add(new IntegerConverter());
            vaccineData = JsonSerializer.Deserialize<VaccineData>(s3contents, serializerOptions);
        }
        else
        {
            Console.WriteLine("Type tag invalid");
            return null;
        }

        // check data objects valid
        if (vaccineData == null)
        {
            Console.WriteLine("Unable to parse file contents");
            return null;
        }

        NpgsqlConnection conn = null;
        try
        {
            // initial db connection
            conn = OpenConnection();
            InitializeTablesIfNotExisting(conn);

            // check if data already exists in db
            if (HasMatchingData(conn, vaccineData))
            {
                // update existing entry in db if found
                UpdateExistingData(conn, vaccineData);
            }
            else
            {
                // write fresh to db if new
                InsertData(conn, vaccineData);
            }
        }
        catch (NpgsqlException ex)
        {
            Console.WriteLine("Npgsql error: {0}", ex.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        finally
        {
            // close out database connection
            if (conn != null && conn.State.Equals(ConnectionState.Open))
            {
                conn.Close();
                conn.Dispose();
            }
        }
        return null; // TODO: temp
    }

    private string? GetEventObjectFileType(S3EventNotification.S3Entity s3Entity)
    {
        string? fileType = null;
        try
        {
            // set up tagging request
            const string fileTypeTagKey = "File-Type";
            GetObjectTaggingRequest request = new GetObjectTaggingRequest();
            request.BucketName = s3Entity.Bucket.Name;
            request.Key = s3Entity.Object.Key;

            // execute tagging request
            Task<GetObjectTaggingResponse> response = this.S3Client.GetObjectTaggingAsync(request);
            if (response != null)
            {
                List<Tag> s3tagging = response.Result.Tagging;
                s3tagging.ForEach(x =>
                {
                    if (x.Key == fileTypeTagKey)
                    {
                        // store tagging response
                        fileType = x.Value;
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading object tag {s3Entity.Object.Key} from bucket {s3Entity.Bucket.Name}.");
            Console.WriteLine(ex.Message);
        }
        return fileType;
    }

    private string? GetEventObjectContents(S3EventNotification.S3Entity s3Entity)
    {
        string? s3contents = null;
        try
        {
            // execute contents request
            Task<GetObjectResponse> response = this.S3Client.GetObjectAsync(s3Entity.Bucket.Name, s3Entity.Object.Key);
            if (response != null)
            {
                using (StreamReader responseReader = new StreamReader(response.Result.ResponseStream))
                {
                    // store contents response
                    s3contents = responseReader.ReadToEnd();
                    responseReader.Close();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading object contents {s3Entity.Object.Key} from bucket {s3Entity.Bucket.Name}.");
            Console.WriteLine(ex.Message);
        }
        return s3contents;
    }

    private VaccineData? ProcessXml(String xmlContents)
    {
        // process xml
        XmlDocument doc = new XmlDocument();
        VaccineData? vaccineData = new VaccineData();
        try
        {
            doc.LoadXml(xmlContents);
            XmlElement dataElement = doc.SelectSingleNode("data") as XmlElement;
            Date vaccineDate = new Date();
            vaccineDate.month = Int32.Parse(dataElement.GetAttribute("month"));
            vaccineDate.day = Int32.Parse(dataElement.GetAttribute("day"));
            vaccineDate.year = Int32.Parse(dataElement.GetAttribute("year"));
            vaccineData.date = vaccineDate;
            XmlElement siteElement = doc.SelectSingleNode("data/site") as XmlElement;
            Site vaccineSite = new Site();
            vaccineSite.id = Int32.Parse(siteElement.GetAttribute("id"));
            vaccineSite.name = siteElement.SelectSingleNode("name").InnerText;
            vaccineSite.zipCode = siteElement.SelectSingleNode("zipCode").InnerText;
            vaccineData.site = vaccineSite;
            XmlNodeList vaccineNodes = doc.SelectNodes("data/vaccines/brand");
            Vaccine[] vaccineArray = new Vaccine[vaccineNodes.Count];
            for (int i = 0; i < vaccineNodes.Count; i++)
            {
                Vaccine vaccineItem = new Vaccine();
                XmlElement vaccineElement = vaccineNodes.Item(i) as XmlElement;
                vaccineItem.brand = vaccineElement.GetAttribute("name");
                vaccineItem.total = Int32.Parse(vaccineElement.SelectSingleNode("total").InnerText);
                vaccineItem.firstShot = Int32.Parse(vaccineElement.SelectSingleNode("firstShot").InnerText);
                vaccineItem.secondShot = Int32.Parse(vaccineElement.SelectSingleNode("secondShot").InnerText);
                vaccineArray.SetValue(vaccineItem, i);
            }
            vaccineData.vaccines = vaccineArray;
        }
        catch (XmlException ex)
        {
            Console.WriteLine(ex.Message);
            vaccineData = null;
        }
        return vaccineData;
    }
    private NpgsqlConnection OpenConnection()
    {
        // initialize sql connection, based on environmental variables
        string? endpoint = Environment.GetEnvironmentVariable("AWS_RDS_ENDPOINT");
        string? dbName = Environment.GetEnvironmentVariable("AWS_RDS_DBNAME");
        string? user = Environment.GetEnvironmentVariable("AWS_RDS_USERNAME");
        string? pass = Environment.GetEnvironmentVariable("AWS_RDS_PASSWORD");

        if (string.IsNullOrEmpty(endpoint) || string.IsNullOrEmpty(dbName) || string.IsNullOrEmpty(user) || string.IsNullOrEmpty(pass))
        {
            throw new Exception("Invalid aws rds access data");
        }

        string connString = "Server=" + endpoint + ";" +
            "port=5432;" +
            "Database=" + dbName + ";" +
            "User Id=" + user + ";" +
            "password=" + pass + ";" +
            "Timeout=10";

        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();
        return conn;
    }

    private void InitializeTablesIfNotExisting(NpgsqlConnection conn)
    {
        // Create sites table
        string createSitesTableCommand = @"
        CREATE TABLE IF NOT EXISTS Sites (
            SiteId INT PRIMARY KEY,
            Name VARCHAR (255),
            ZipCode VARCHAR(20)
        )";
        NpgsqlCommand dbCreateSitesTable = new NpgsqlCommand(createSitesTableCommand, conn);
        dbCreateSitesTable.ExecuteNonQuery();

        // Create data table
        string createDataTableCommand = @"
        CREATE TABLE IF NOT EXISTS Data(
            SiteId INT REFERENCES Sites (SiteId),
            Date DATE,
            FirstShot INT,
            SecondShot INT,
            PRIMARY KEY (SiteId, Date)
        )";
        NpgsqlCommand dbCreateDataTable = new NpgsqlCommand(createDataTableCommand, conn);
        dbCreateDataTable.ExecuteNonQuery();
    }

    private bool HasMatchingData(NpgsqlConnection conn, VaccineData vaccineData)
    {
        // check if the data table contains a matching entry
        string selectVaccineDataCommand = @"
            SELECT COUNT(1)
            FROM Data
            WHERE SiteId = ($1) AND Date = ($2)
        ";
        NpgsqlCommand dbSelectVaccineData = new NpgsqlCommand(selectVaccineDataCommand, conn)
        {
            Parameters =
            {
                new() {Value = vaccineData.site.id},
                new() {Value = getDateAsDateTime(vaccineData)}
            }
        };
        Int64 matchCount;
        try
        {
            matchCount = (Int64)dbSelectVaccineData.ExecuteScalar();
        }
        catch (Exception ex)
        {
            matchCount = 0;
        }
        return matchCount > 0;
        
    }

    private bool HasMatchingSite(NpgsqlConnection conn, VaccineData vaccineData)
    {
        // check if the site table containes a matching entry
        string selectVaccineSiteCommand = @"
            SELECT COUNT(1)
            FROM Sites
            WHERE SiteId = ($1)
        ";
        NpgsqlCommand dbSelectVaccineSite = new NpgsqlCommand(selectVaccineSiteCommand, conn)
        {
            Parameters =
            {
                new () { Value = vaccineData.site.id}
            }
        };
        Int64 matchCount;
        try
        {
            matchCount = (Int64)dbSelectVaccineSite.ExecuteScalar();
        }
        catch (Exception ex)
        {
            matchCount = 0;
        }
        return matchCount > 0;
    }

    private void UpdateExistingData(NpgsqlConnection conn, VaccineData vaccineData)
    {
        // update an existing entry in the data table
        string updateVaccineDataCommand = @"
            UPDATE Data
            SET FirstShot=($1), SecondShot=($2)
            WHERE SiteId=($3) AND Date=($4)
        ";
        int[] shotSums = getShotSums(vaccineData);
        NpgsqlCommand dbUpdateVaccineData = new NpgsqlCommand(updateVaccineDataCommand, conn)
        {
            Parameters =
            {
                new() { Value = shotSums[0]},
                new() { Value = shotSums[1]},
                new() { Value = vaccineData.site.id},
                new() { Value = getDateAsDateTime(vaccineData)}
            }
        };
        dbUpdateVaccineData.ExecuteNonQuery();
    }

    private void InsertData(NpgsqlConnection conn, VaccineData vaccineData)
    {
        // add new vaccine data to the database
        if(!HasMatchingSite(conn, vaccineData))
        {
            // create a new site entry if this is the first time data from this site is being added
            string insertVaccineSiteCommand = @"
                INSERT INTO Sites (SiteId, Name, ZipCode)
                VALUES (($1), ($2), ($3))
            ";
            NpgsqlCommand dbInsertVaccineSite = new NpgsqlCommand(insertVaccineSiteCommand, conn)
            {
                Parameters =
                {
                    new() {Value = vaccineData.site.id},
                    new() {Value = vaccineData.site.name},
                    new() {Value = vaccineData.site.zipCode}
                }
            };
            dbInsertVaccineSite.ExecuteNonQuery();
        }
        // add new data entry
        string insertVaccineDataCommand = @"
            INSERT INTO Data (SiteId, Date, FirstShot, SecondShot)
            VALUES (($1), ($2), ($3), ($4))
        ";
        int[] shotSums = getShotSums(vaccineData);
        NpgsqlCommand dbInsertVaccineData = new NpgsqlCommand(insertVaccineDataCommand, conn)
        {
            Parameters =
            {
                new() {Value = vaccineData.site.id},
                new() {Value = getDateAsDateTime(vaccineData)},
                new() {Value = shotSums[0]},
                new() {Value = shotSums[1]}
            }
        };
        dbInsertVaccineData.ExecuteNonQuery();
    }

    private int[] getShotSums(VaccineData vaccineData)
    {
        // helper function to sum the total number of first shots and second shots among all vaccine brands in a data set
        int[] shotArray = new int[2];
        foreach (Vaccine vaccine in vaccineData.vaccines)
        {
            shotArray[0] += (int)vaccine.firstShot;
            shotArray[1] += (int)vaccine.secondShot;
        }
        return shotArray;
    }

    private DateTime getDateAsDateTime(VaccineData vaccineData)
    {
        // helper function to convert custom year-month-day data class into a DateTime object for postgres storage
        return new DateTime((int)vaccineData.date.year, (int)vaccineData.date.month, (int)vaccineData.date.day);
    }

}
