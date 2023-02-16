using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    static class Program
    {
        static void Main()
        {
            string csv_file_path = @"C:\Users\AnilKumarBN\OneDrive - Anthology Inc\StudentDetails.csv";
            DataTable csvData = GetDataTabletFromCSVFile(csv_file_path);
            InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);
                      
        }
        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                
            }
            Console.WriteLine("Running Successfully");

            return csvData;
        }
        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData)
        {
            using (SqlConnection dbConnection = new SqlConnection(@"Data Source=LPTIN-40HYZ53;Initial Catalog=StudentImport;Integrated Security=True;"))
            {
                dbConnection.Open();
                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    try
                    {
                        s.DestinationTableName = "Studentdetails";
                        s.WriteToServer(csvFileData);
                    }
                    catch(Exception exc)
                    {
                        Console.WriteLine("Duplicate Record Trying to Insert");
                        
                    }
                }
            }
        }

    }
}