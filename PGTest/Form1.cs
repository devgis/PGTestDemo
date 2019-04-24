using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Npgsql;

namespace PGTest
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button3_Click(object sender, EventArgs e)
        {
            if(NpgsqlHelper.ExecSql("delete from test")>0)
            {
                MessageBox.Show("删除成功！");
            }
            else
            {
                MessageBox.Show("删除失败！");
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            dataGridView1.DataSource = NpgsqlHelper.GetDataTable("select * from test");
        }
        Random rd = new Random();
        private void button1_Click(object sender, EventArgs e)
        {


            string id = Guid.NewGuid().ToString();
            string name = "name" + rd.Next();
            int age = rd.Next();
            string other = "other" + rd.Next();

            string sql = "insert into test (id,name,age,inputdate,other)values(:id,:name,:age,:inputdate,:other)";
            NpgsqlParameter[] parameters = new NpgsqlParameter[] { 
                new NpgsqlParameter("id",DbType.String),
                new NpgsqlParameter("name",DbType.String),
                new NpgsqlParameter("age",DbType.Int32),
                new NpgsqlParameter("inputdate",DbType.DateTime),
                new NpgsqlParameter("other",DbType.String)
            };
            parameters[0].Value = id;
            parameters[1].Value = name;
            parameters[2].Value = age;
            parameters[3].Value = DateTime.Now;
            parameters[4].Value = other;


            if (NpgsqlHelper.ExecSql(sql, parameters) > 0)
            {
                MessageBox.Show("添加成功！");
            }
            else
            {
                MessageBox.Show("添加失败！");
            }
        }

        private void button4_Click(object sender, EventArgs e)
        {
            List<string> listsqls = new List<string>();
            List<NpgsqlParameter[]> listparams = new List<NpgsqlParameter[]>();
            for (int i = 0; i < 1000000; i++)
            {
                string id = Guid.NewGuid().ToString();
                string name = "name" + rd.Next();
                int age = rd.Next();
                string other = "other" + rd.Next();

                string sql = "insert into test (id,name,age,inputdate,other)values(:id,:name,:age,:inputdate,:other)";
                NpgsqlParameter[] parameters = new NpgsqlParameter[] { 
                    new NpgsqlParameter("id",DbType.String),
                    new NpgsqlParameter("name",DbType.String),
                    new NpgsqlParameter("age",DbType.Int32),
                    new NpgsqlParameter("inputdate",DbType.DateTime),
                    new NpgsqlParameter("other",DbType.String)
                };
                parameters[0].Value = id;
                parameters[1].Value = name;
                parameters[2].Value = age;
                parameters[3].Value = DateTime.Now;
                parameters[4].Value = other;

                listsqls.Add(sql);
                listparams.Add(parameters);

            }

            DateTime time1 = DateTime.Now;
            if (NpgsqlHelper.ExecSqlByTran(listsqls, listparams))
            {
                TimeSpan ts = DateTime.Now - time1;
                MessageBox.Show("成功，耗时:" + ts.TotalSeconds + "秒");
            }
            else
            {
                MessageBox.Show("失败！");
            }

        }
    }
}
