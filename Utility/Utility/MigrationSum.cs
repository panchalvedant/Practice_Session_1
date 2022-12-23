using System;
using System.Data.SqlClient;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlTypes;
using System.Reflection;
using System.Data;
using System.Diagnostics;

namespace Utility
{
    public class MigrationSum
    {
        private const string Conn = @"Data Source=(LocalDB)\MSSQLLocalDB;Initial Catalog=Practise;Integrated Security=True";
        public static int start_value, end_value, range, batch, count;
        public static bool completed = false;
        public static bool con = false;
        public static SqlDataReader sr;
        static SqlConnection sql1;

        static void Main(string[] args)
        {
            string again;
            do
            {
                Read_Value();
                Cancel_Status();
                Console.WriteLine("Do you want to continue ? then type  (YES/NO)");
                again = Console.ReadLine();
            }
            while (again == "YES" || again == "yes");
        }

        private static void Read_Value()
        {
            bool value = true;
            do
            {
                Console.WriteLine("Enter the statring value of id");
                while (!(Int32.TryParse(Console.ReadLine(), out start_value)))
                {
                    Console.WriteLine("Invalid Data value for start id Re-enter the data");
                }

                Console.WriteLine("Enter the ending value of id");
                while (!(Int32.TryParse(Console.ReadLine(), out end_value)))
                {
                    Console.WriteLine("Invalid Data ending value of id re-enter the data");
                }

                range = end_value - start_value + 1;
                count = start_value - 1;

                if ((start_value > end_value) || (range > 1000000))
                {
                    Console.WriteLine("...Invalid Data Entry...");
                    value = true;
                }
                else
                {
                    value = false;
                }
            } while (value);
            

        }
        
        public static void Cancel_Status()
        {
            if ((start_value == 0) || (end_value == 0))
            {
                Console.WriteLine("Invalid Data");
            }
            else
            {
                Thread backgroundThread = new Thread(Migration);
                backgroundThread.IsBackground = true;
                backgroundThread.Start();
                do
                {
                    string input = Console.ReadLine();
                   
                    if (input.ToUpper() == "CANCEL")
                    {
                        Console.WriteLine("-------------------STATUS-------------------");
                        Console.WriteLine($" Migrated Data :- {count} ");
                        Console.WriteLine($" Cancel Data :- {end_value - count} ");
                        con = true;
                        break;
                    }
              
                    else if (input.ToUpper() == "STATUS")
                    {
                        Console.WriteLine("-------------------STATUS-------------------");
                        Console.WriteLine($" Migrated Data :- {count} ");
                        Console.WriteLine($" Ongoing Data :- {end_value - count} ");
                        Console.WriteLine(" ");
                    }
                    else if (completed)
                    {
                        Console.WriteLine("-------------------STATUS-------------------");
                        Console.WriteLine($" Migrated Data :- {end_value - start_value}  ");
                        Console.WriteLine($" Cancel Data :- {end_value - count} ");
                        break;
                    }

                } while (true);
            }
        }

        public static void Migration()
        {
            var solution = Data_fetch();
            Migration_process(solution);
            Console.WriteLine($"ALL MIGRATION COMPLETED...NOW, PRESS ANY KEY TO SEE YOUR STATUS");
            completed = true;
            if (con == true)
                return;
            sql1.Close();
        }

        public static Dictionary<int, (int, int)> Data_fetch()
        {
            
            sql1 = new SqlConnection(Conn);
            Dictionary<int, (int, int)> dict = new Dictionary<int, (int, int)>();
            SqlCommand command = new SqlCommand();
            int size = end_value - start_value + 1;
            String query = "SELECT * FROM SourceTable ORDER BY ID OFFSET " + (start_value - 1) + " ROWS FETCH NEXT " + size + " ROWS ONLY;";
            command.CommandType = CommandType.Text;
            command.CommandText = query;
            command.Connection = sql1;
            sql1.Open();
            using (SqlDataReader reader = command.ExecuteReader())
            {
                // Another Record insertion
                while (reader.Read())
                {
                    int id = Convert.ToInt32(reader[0]);
                    int num1 = Convert.ToInt32(reader[1]);
                    int num2 = Convert.ToInt32(reader[2]);
                    dict[id] = (num1, num2);
                }
            }
            sql1.Close();
            return dict;
        }

        public static void Migration_process(Dictionary<int, (int, int)> dict1)
        {
            Console.WriteLine(" *****************Migration started*********************");
            Console.WriteLine("--------------------------------------------------------");
            Dictionary<int, (int, int)> temp = new Dictionary<int, (int, int)>();
            int cnt = 0;
            foreach (var X in dict1)
            {
                cnt++;
                temp[X.Key] = (X.Value.Item1, X.Value.Item2);
                if (cnt == 100)
                {
                  
                    Batch_Processing(temp);
                    Console.WriteLine("-------------------------------------------------");
                    Console.WriteLine($"{cnt} data batch is completed");
                    Console.WriteLine("-------------------------------------------------");
                    temp.Clear();
                    cnt = 0;
                    if (con == true)
                        return;
                }
            }

            if (cnt != 0)
            {
                Batch_Processing(temp);
            }
        }

        public static void Batch_Processing(Dictionary<int, (int, int)> dict2)
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("Source_Id", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("Sum", typeof(Int32)));
            foreach (var Y in dict2)
            {
                int id = Y.Key;
                int num1 = Y.Value.Item1;
                int num2 = Y.Value.Item2;
                DataRow dr = tbl.NewRow();
                dr["Source_Id"] = id;
                dr["Sum"] = num1 + num2;
                tbl.Rows.Add(dr);
            }
            SqlBulkCopy objbulk = new SqlBulkCopy(sql1);
            objbulk.DestinationTableName = "DestinatioTable";
            objbulk.ColumnMappings.Add("Source_Id", "Source_Id");
            objbulk.ColumnMappings.Add("Sum", "Sum");
            sql1.Open();
            //INSERT BULK RECORDS;
            try
            {
                objbulk.WriteToServer(tbl);
                count += tbl.Rows.Count;
                Console.WriteLine($"{tbl.Rows.Count} data batch is inserted into DB");
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                sql1.Close();
            }
        }
    }
}
