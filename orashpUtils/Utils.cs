using System;
using System.IO;

namespace orashpUtils
{
    public static class Utils
    {
        public static string ParseConnectionString(string constring)
        {
            string dbalias = constring.Split('@')[1];
            string username = constring.Split('/')[0];
            string password = constring.Split('/')[1].Split('@')[0];
            string retString = "Data Source=" + dbalias + ";User Id=" + username + ";Password=" + password + ";";
            return retString;
        }

        public static void WriteErrLog(Exception ex)
        {
            StreamWriter objStreamWriter;
            objStreamWriter = File.AppendText("orashp_err.log");
            //Append the the current date and time at the end of the string followed by the exception"
            objStreamWriter.WriteLine(DateTime.Now.ToString() + " ERROR: " + ex.Message);
            objStreamWriter.WriteLine("\n " + ex.StackTrace + "\n");
            objStreamWriter.WriteLine("#######################################################################################");
            //Close the stream
            objStreamWriter.Close();
        }
    }
}