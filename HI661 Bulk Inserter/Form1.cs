using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;

namespace HI661_Bulk_Inserter
{
    public partial class Form1 : Form
    {
        string myPathToScan;
        const string columnDelimiter = "|:|";
        const string myDBconnString = "Data Source=.\\SQLEXPRESS;Initial Catalog=HCAHPS_CS1;Integrated Security=True";
        string output = "";
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            this.Cursor = Cursors.WaitCursor;
            output += "Starting Scan for files...\r\n";
            label1.Text = "Starting Scan for files...";
            WalkDirectoryTree(new DirectoryInfo(myPathToScan));
            output += "DONE.\n";

            File.WriteAllText(myPathToScan + "\\output.txt", output);

            MessageBox.Show("Done!");
            label1.Text = "";
            this.Cursor = Cursors.Default;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            folderBrowserDialog1.ShowDialog();
            myPathToScan = folderBrowserDialog1.SelectedPath;
            textBox1.Text = myPathToScan;

        }

        private void WalkDirectoryTree(System.IO.DirectoryInfo root)
        {
            System.IO.FileInfo[] files = null;
            System.IO.DirectoryInfo[] subDirs = null;

            // First, process all the files directly under this folder
            try
            {
                files = root.GetFiles("*.*");
            }
            // This is thrown if even one of the files requires permissions greater
            // than the application provides.
            catch (UnauthorizedAccessException e)
            {
                // This code just writes out the message and continues to recurse.
                // You may decide to do something different here. For example, you
                // can try to elevate your privileges and access the file again.
                MessageBox.Show(e.Message);
            }

            catch (System.IO.DirectoryNotFoundException e)
            {
                Console.WriteLine(e.Message);
            }

            if (files != null)
            {
                foreach (System.IO.FileInfo fi in files)
                {
                    if (fi.Extension == ".csv")
                    {
                        Console.WriteLine("Loading " + fi.FullName);
                        label1.Text = "Loading " + fi.FullName;
                        this.Refresh();
                        var result = LoadFileIntoDB(fi);
                        output += result + "\r\n";
                    }
                }

                // Now find all the subdirectories under this directory.
                subDirs = root.GetDirectories();

                foreach (System.IO.DirectoryInfo dirInfo in subDirs)
                {
                    // Resursive call for each subdirectory.
                    WalkDirectoryTree(dirInfo);
                }
            }
        }

        private void button3_Click(object sender, EventArgs e)
        {
            string path = "C:\\Users\\ntimko\\Desktop\\Case Study\\2014\\20140717\\HCAHPS - Hospital.csv";

            //string fileContents = File.ReadAllText(path);
            //File.WriteAllText(path, cleanUpDelimiters(fileContents));

            //MessageBox.Show("Done!");

            FileInfo myNewFile = new FileInfo(path);
            MessageBox.Show(LoadFileIntoDB(myNewFile));
        }

        private string LoadFileIntoDB(FileInfo myFile)
        {
            string cleanContents = PrepFile(myFile);

            char[] splitChars = { '\n' };

            string[] textRows = cleanContents.Split(splitChars);

            char[] splitChars2 = { '\\' };

            string[] temp = myFile.DirectoryName.Split(splitChars2);
            
            string TableName = myFile.Name.Replace(" - ", "_").Replace(' ', '_').Replace(myFile.Extension, "") + "_" + temp[temp.Length - 1];
            string[] columns;

            if (myFile.Name.Contains("hvbp"))
            {
                columns = textRows[0].Split(','); //hbvp files have headers that are delimited with just commas
            }
            else
            {
                columns = textRows[0].Split(new string[] { columnDelimiter }, StringSplitOptions.None);
            }

            CreateSQLTable(TableName, columns);
            var loadResult = LoadTableFromFile(textRows, columns, TableName);

            return loadResult;
        }

        private string PrepFile(FileInfo myFile)
        {
            string fileContents = File.ReadAllText(myFile.FullName);
            return cleanUpDelimiters(fileContents);
        }
        

        private string cleanUpDelimiters(string fileContents)
        {
            //return fileContents.Replace("\",\"", columnDelimiter).Replace("\"\r\n\"", "\n").Trim('\n').Trim('\r').Trim('"');
            return fileContents.Replace("\",\"", columnDelimiter).Replace("\"\r\n\"", "\n").Replace("\"\n\"", "\n").Replace("\r\n\"", "\n").Replace("\n\"", "\n").Trim('\n').Trim('\r').Trim('"');
        }

        private void CreateSQLTable(string TableName, string[] columnNames)
        {
            if (String.IsNullOrEmpty(TableName) || TableName == null)
            {
                throw new Exception("Cannot create the current table in the DB without a name or columns");
            }
            //else continue


            using (SqlConnection myConn = new SqlConnection(myDBconnString))
            {
                SqlCommand myCommand = new SqlCommand("SELECT 'x' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '" + TableName + "'", myConn);
                myCommand.CommandType = CommandType.Text;

                myConn.Open();

                //check if the table is there
                var result = myCommand.ExecuteScalar();

                if (result != null)
                {
                    //table exists, so let's drop it
                    myCommand.CommandText = "DROP TABLE " + TableName;
                    myCommand.ExecuteNonQuery();
                }

                //create the table

                //first, create the column list
                string columnDefinitions = "";
                foreach (string colName in columnNames)
                {
                    columnDefinitions += "[" + colName + "] VARCHAR(500) NULL, ";
                }

                char[] trimChars = { ' ', ',' };

                //strip off the last comma and space
                columnDefinitions = columnDefinitions.TrimEnd(trimChars);

                //create table CREATE statement
                string query = "CREATE TABLE dbo." + TableName + " (" + columnDefinitions + ")";

                myCommand.CommandText = query;
                myCommand.ExecuteNonQuery();

                myConn.Close();
            }
        }

        private string LoadTableFromFile(string[] textRows, string[] columnNames, string TableName)
        {
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(myDBconnString, SqlBulkCopyOptions.TableLock)
            {
                DestinationTableName = TableName,
                BulkCopyTimeout = 0,
                BatchSize = 1000
            })
            {
                //article on efficient loading of large numbers of records into a SQL table from text
                //https://gallery.technet.microsoft.com/scriptcenter/Import-Large-CSVs-into-SQL-216223d9

                using (DataTable datatable = new DataTable())
                {
                    var columns = datatable.Columns;

                    foreach (string colName in columnNames) // should be in same order as used in the CreateSQLTable function, so we're good
                    {
                        columns.Add(colName, typeof(System.String));
                    }

                    int batchsize = 0;
                    int totalRows = 0;

                    var newRowList = textRows.Skip(1).ToArray(); // skip the first row because it contains column names
                    
                    foreach(string rowOfText in newRowList)
                    {
                        //we load the values we need into the DataTable
                        var splitLine = rowOfText.Split(new string[] { columnDelimiter }, StringSplitOptions.None);

                        List<string> newRowValues = new List<string>();
                        
                        for(int i=0; i < splitLine.Length; i++)
                        {
                            newRowValues.Add(splitLine[i].Replace("\"\"", "\""));
                        }
                        

                        datatable.Rows.Add(newRowValues.ToArray());
                        
                        batchsize++;
                        if (batchsize == 100)
                        {
                            bulkcopy.WriteToServer(datatable);
                            datatable.Rows.Clear();
                            batchsize = 0;
                        }

                        totalRows++;
                        
                    }

                    //write one last time, in case we broke out of the loop while there were still records waiting to be bulk inserted
                    bulkcopy.WriteToServer(datatable);
                    datatable.Rows.Clear();

                    return TableName + " was loaded with " + totalRows.ToString() + " total rows.\n";
                }

            }
        }
    }
}
