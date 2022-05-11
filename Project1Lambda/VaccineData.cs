using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*
 * Code by Andrew
 */

namespace Project1Lambda
{
    // A simple data class used to store vaccine info, parsed from xml or json by the lambda function
    public  class VaccineData
    {
        public Date? date { get; set; }
        public Site? site { get; set; }
        public Vaccine[]? vaccines { get; set; }
    }

    public class Date
    {
        public int? month { get; set; }
        public int? day { get; set; }
        public int? year { get; set; }
    }

    public class Site
    {
        public int? id { get; set; }
        public string? name { get; set; }
        public string? zipCode { get; set; }
    }

    public class Vaccine
    {
        public string? brand { get; set; }
        public int? total { get; set; }
        public int? firstShot { get; set; }
        public int? secondShot { get; set; }
    }
}
