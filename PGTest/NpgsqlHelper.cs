using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Threading;
using Npgsql;

namespace PGTest
{
    public static class NpgsqlHelper
    {
        private static NpgsqlConnection SqlConn;
        static NpgsqlHelper()
        {
            if (SqlConn == null)
            {
                SqlConn = new NpgsqlConnection();
                SqlConn.ConnectionString = System.Configuration.ConfigurationManager.AppSettings["pgstr"].ToString();
            }
        }
        //public static string connectionString = "Data Source=" + ostr + ";Persist Security Info=True;User ID=" + ustr + ";Password=" + pstr + ";Unicode=True;Pooling=true;Min Pool Size=50;Max Pool Size=500;Connection Lifetime=1;";   //Connection Lifetime=10000;

        public static NpgsqlDataReader GetDataReader(string sql)
        {
            try
            {
                NpgsqlCommand sqlCmd = new NpgsqlCommand();
                sqlCmd.CommandText = sql;
                sqlCmd.Connection = SqlConn;
                NpgsqlDataReader sqlDr = sqlCmd.ExecuteReader(CommandBehavior.CloseConnection);
                sqlDr.Read();
                return sqlDr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataTable GetDataTable(string sql, NpgsqlParameter[] parameters)
        {

            try
            {
                DataSet ds = GetDataSet(sql, parameters);
                if (ds != null && ds.Tables.Count > 0)
                {
                    return ds.Tables[0];
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataSet GetDataSet(string sql, NpgsqlParameter[] parameters)
        {

            try
            {
                DataSet ds = new DataSet();
                NpgsqlCommand sqlCmd = new NpgsqlCommand();
                sqlCmd.CommandText = sql;
                sqlCmd.Parameters.AddRange(parameters);
                sqlCmd.Connection = SqlConn;

                NpgsqlDataAdapter da = new NpgsqlDataAdapter(sqlCmd);
                da.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SqlConn.Close();
            }
        }

        public static DataTable GetDataTable(string sql)
        {

            try
            {
                DataSet ds = GetDataSet(sql);
                if (ds != null && ds.Tables.Count > 0)
                {
                    return ds.Tables[0];
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SqlConn.Close();
            }
        }

        public static int ExecSql(string sql)
        {
            try
            {
                NpgsqlCommand sqlCmd = new NpgsqlCommand();
                sqlCmd.CommandText = sql;
                sqlCmd.Connection = SqlConn;
                SqlConn.Open();
                int i = sqlCmd.ExecuteNonQuery();
                return i;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SqlConn.Close();
            }
        }

        public static int ExecSql(string sql, NpgsqlParameter[] parameters)
        {
            try
            {
                SqlConn.Open();
                NpgsqlCommand sqlCmd = new NpgsqlCommand();
                sqlCmd.CommandText = sql;
                sqlCmd.Parameters.AddRange(parameters);
                sqlCmd.Connection = SqlConn;
                int i = sqlCmd.ExecuteNonQuery();
                return i;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SqlConn.Close();
            }
        }

        public static bool ExecSqlByTran(List<string> listsqls, List<NpgsqlParameter[]> listparameters)
        {
            if (listsqls == null || listparameters == null || listsqls.Count <= 0 || listparameters.Count <= 0 || listsqls.Count != listparameters.Count)
            {
                return false;
            }
            else
            {
                
                SqlConn.Open();
                NpgsqlTransaction tran = SqlConn.BeginTransaction();
                try
                {
                    for (int i = 0; i < listsqls.Count; i++)
                    {
                        NpgsqlCommand sqlCmd = new NpgsqlCommand();
                        sqlCmd.CommandText = listsqls[i];
                        sqlCmd.Parameters.AddRange(listparameters[i]);
                        sqlCmd.Connection = SqlConn;
                        sqlCmd.Transaction = tran;
                        sqlCmd.ExecuteNonQuery();
                    }
                    tran.Commit();
                    SqlConn.Close();
                    return true;
                }
                catch(Exception ex)
                {
                    tran.Rollback();
                    SqlConn.Close();
                    throw ex;
                    //return false;
                }
                
            }
        }

        public static int ExecSql(string sql, NpgsqlConnection con)
        {
            try
            {
                NpgsqlCommand sqlCmd = new NpgsqlCommand();
                sqlCmd.CommandText = sql;
                sqlCmd.Connection = con;
                int i = sqlCmd.ExecuteNonQuery();
                return i;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string getConfig(string sConfigStr)
        {
            NpgsqlDataReader odr = null;
            try
            {
                odr = GetDataReader("select value from config where name='" + sConfigStr + "'");
                string sValue = odr.GetString(0);
                return sValue;
            }
            catch(Exception ex)
            {
                return "";
            }
            finally
            {
                if (!odr.IsClosed)
                {
                    odr.Close();
                }
            }
        }

        

        public static DataSet GetDataSet(string sql)
        {
            try
            {
                NpgsqlDataAdapter oda = new NpgsqlDataAdapter();
                DataSet ds = new DataSet();
                NpgsqlCommand sqlcmd = new NpgsqlCommand(sql, SqlConn);
                oda.SelectCommand = sqlcmd;
                oda.Fill(ds);
                return ds;
            }
            catch(Exception ex)
            {
                return null;
            }

        }
        static NpgsqlConnection conn = null;
        public static NpgsqlConnection OracleCon()
        {
            if (conn == null)
            {
                conn = new NpgsqlConnection();
                conn.ConnectionString = System.Configuration.ConfigurationManager.AppSettings["OracleStr"].ToString();
            }
            return conn;
        }
    }
}
