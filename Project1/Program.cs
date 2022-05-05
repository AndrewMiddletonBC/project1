using System;
using Amazon.S3;
using Amazon.S3.Model;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon;

namespace Project1
{
    class Program
    {
        private const string BucketName = "thien-andrew-project1";
        private static readonly RegionEndpoint BucketRegion = RegionEndpoint.USEast1;
        private static IAmazonS3 _client;
 
        static void Main(string[] args)
        {
            Console.WriteLine(args[0]);
            _client = new AmazonS3Client(BucketRegion);
            PutObjectWithTagsTestAsync(args).Wait();
        }
        static async Task PutObjectWithTagsTestAsync(string[] args)
        {
            try
            {
                string fileType;
                string[] splitString = Regex.Split(args[0], @"\\"); 
                if(args[1] == "xml")
                {
                    fileType = "xml";
                }
                else
                {
                    fileType = "json";
                }
                var putRequest = new PutObjectRequest
                {
                    BucketName = BucketName,
                    Key = splitString[splitString.Length - 1],
                    FilePath = args[0],
                    TagSet = new List<Tag>{
                        new Tag { Key = "File-Type", Value = fileType},
                    }
                };
                PutObjectResponse response = await _client.PutObjectAsync(putRequest);
                GetObjectTaggingRequest getTagsRequest = new GetObjectTaggingRequest
                {
                    BucketName = BucketName,
                    Key = splitString[splitString.Length - 1]
                };

                GetObjectTaggingResponse objectTags = await _client.GetObjectTaggingAsync(getTagsRequest);
                

            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine(
                        "Error encountered ***. Message:'{0}' when writing an object"
                        , e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine(
                    "Encountered an error. Message:'{0}' when writing an object"
                    , e.Message);
            }
        }
    }
}