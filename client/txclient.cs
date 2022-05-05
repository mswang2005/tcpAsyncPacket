using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using tcpAsyncPacket;
namespace client
{
    public partial class txclient : Form
    {
        private tcpAsyncClient client = null;
        private ipcClient ipckhd = null;

        public txclient()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (client==null)
            {
                client = new tcpAsyncClient();
                client.connectResult += Client_connectResult;
                client.disConnected += Client_disConnected;
                client.recData += Client_recData;
                client.recFileRate += Client_recFileRate;
                client.recFileSpeed += Client_recFileSpeed;
                client.sendFileRate += Client_sendFileRate;
                client.sendFileSpeed += Client_sendFileSpeed;
                client.sendOver += Client_sendOver;
                client.errorRec += Client_errorRec;
            }
            if (button1.Text=="连接")
            {
                if (client.connect(new IPEndPoint(IPAddress.Parse(textBox1.Text), int.Parse(textBox2.Text))))
                {
                    label3.Text = client.lcEDP.ToString();
                    button1.Text = "断开";
                    ipckhd = new ipcClient(client.lcEDP.ToString());
                    ipckhd.OnRecieve += Ipckhd_OnRecieve;
                    ipckhd.start();
                }
                else
                {
                    label3.Text = "连接失败";
                }
            }
            else
            {
                ipckhd.stop();
                client.disConnect();
                button1.Text = "连接";
            }
          
           
        }

        private object Ipckhd_OnRecieve(string sender, ipcArg e)
        {
            var tstr = e.getData<string>();
            voke(() => listBox1.Items.Add("ipc:" + sender + ":" + tstr));
            if (tstr == "time")
            {
                return DateTime.Now;
            }
            else
            {
                return null;
            }


        }

        private void Client_sendOver(object sender, infoArg e)
        {
            if (e.iType==infoType.cli_sendfile||e.iType==infoType.cli_sendfileP2P)
            {
                if (e.isok)
                {
                    listBox1.Items.Add("文件发送成功！");
                }
                else
                {
                    listBox1.Items.Add("文件发送失败！");
                }
            }

        }

        private object Client_recData(IPEndPoint sender, netArg e)
        {
            if (e.isFile)
            {
                if (e.isOk)
                {
                    voke(() => listBox1.Items.Add(e.getData<string>() + " 已接收"));
                }
                return null;
            }
            else
            {
                switch (e.userToken)
                {
                    case 0:
                        var tstr = e.getData<string>();
                        if (tstr.Contains('|'))
                        {
                            var wjsz = tstr.Split('|');
                            if (wjsz[0] == "sendfile")
                            {
                                client.getFile(wjsz[1]);
                            }
                            else
                            {
                                voke(() => listBox1.Items.Add(sender.ToString() + ":" + tstr));
                            }
                            return null;
                        }
                        else
                        {
                            voke(() => listBox1.Items.Add(sender.ToString() + ":" + tstr));
                            if (tstr == "time")
                            {
                                return DateTime.Now;
                            }
                            else
                            {
                                return null;
                            }

                        }
                    case 1:
                        var lst = e.getData<HashSet<khlx>>();
                        voke(() => dataGridView1.DataSource = lst.ToList());
                        return null;
                    default:
                        return null;

                }
            }
        }

        private void Client_connectResult(object sender, infoArg e)
        {
            this.BeginInvoke(new Action(()=> button1.Text = "断开"));
            
        }

        private void Client_errorRec(object sender, infoArg e)
        {
            this.BeginInvoke(new Action(()=>listBox2.Items.Add(e.message)));
        }



        private void Client_sendFileSpeed(object sender, fileArg e)
        {
            voke(() => label2.Text = e.str);
        }

        private void Client_sendFileRate(object sender, fileArg e)
        {
            voke(() => label1.Text = e.str);
        }

        private void Client_recFileSpeed(object sender, fileArg e)
        {
            voke(() => label2.Text = e.str);
        }

        private void voke(Action inact)
        {
            this.BeginInvoke(inact);
        }
        private void Client_recFileRate(object sender, fileArg e)
        {

            voke(() => label1.Text = e.str);
            
        }


        private void Client_disConnected(object sender, infoArg e)
        {
            button1.Text = "连接";
            ipckhd.stop();
        }





        private void button2_Click(object sender, EventArgs e)
        {
            if (client!=null&&client.available)
            {
                if (checkBox1.Checked)
                {
                    if (!string.IsNullOrWhiteSpace(textBox4.Text))
                    {
                        ipckhd.sendObject(textBox4.Text, textBox3.Text);
                    }
                }
                else
                {
                    var ip = textBox4.Text.toIP();
                    if (ip == null)
                    {
                        client.sendObject(textBox3.Text);
                    }
                    else
                    {
                        client.sendObjectP2P(ip, textBox3.Text, 0);
                    }
                }

                
            }
        }


        private void button4_Click(object sender, EventArgs e)
        {
            if (client != null && client.available)
            {
                OpenFileDialog of = new OpenFileDialog();
                if (of.ShowDialog() == DialogResult.OK)
                {
                    var ip = textBox4.Text.toIP();
                    if (ip == null)
                    {
                        client.sendFile(of.FileName);
                    }
                    else
                    {
                        client.sendFileP2P(ip, of.FileName);
                    }
                    
                }


            }

        }


        private void button7_Click(object sender, EventArgs e)
        {

            if (client != null && client.available)
            {
                if (checkBox1.Checked)
                {
                    if (!string.IsNullOrWhiteSpace(textBox4.Text))
                    {
                        var dt = ipckhd.getReply<DateTime?>(textBox4.Text,"time");
                        if (dt.HasValue)
                        {
                            MessageBox.Show(dt.ToString());
                        }
                        else
                        {
                            MessageBox.Show("null");
                        }
                        
                    }
                }
                else
                {
                    var ip = textBox4.Text.toIP();
                    if (ip == null)
                    {
                        var dt = client.getReply<DateTime?>("time");
                        if (dt.HasValue)
                        {
                            MessageBox.Show(dt.ToString());
                        }
                        else
                        {
                            MessageBox.Show("null");
                        }
                    }
                    else
                    {
                        var dt = client.getReplyP2P<DateTime?>(ip, "time", 0);
                        if (dt.HasValue)
                        {
                            MessageBox.Show(dt.ToString());
                        }
                        else
                        {
                            MessageBox.Show("null");
                        }
                    }
                }


            }
        }

    }
    public class khlx
    {
        public Guid id { get; set; } = Guid.NewGuid();
        public DateTime dt { get; set; } = DateTime.Now;
    }
    public static class ex
    {
        public static IPEndPoint toIP(this string instr)
        {
            if (string.IsNullOrWhiteSpace(instr))
            {
                return null;
            }
            else
            {
                var tpstr = instr.Trim().Split(':');
                if (tpstr.Length == 2)
                {
                    if (IPAddress.TryParse(tpstr[0], out var ip))
                    {
                        if (int.TryParse(tpstr[1], out var port))
                        {
                            return new IPEndPoint(ip, port);
                        }
                        else
                        {
                            return null;
                        }
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }
        }
    }
}
