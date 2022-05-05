//#define debug
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Ipc;
using System.Runtime.Remoting.Channels;
using System.Collections;
using System.Runtime.Serialization.Formatters;
using System.Reflection;
using System.Numerics;
using System.Reflection.Emit;
using System.Runtime.Serialization;

namespace tcpAsyncPacket
{
    internal class replyIfo
    {
        public AutoResetEvent autoEvent { get; set; } = new AutoResetEvent(false);
        public byte[] bin { get; set; }

    }
    internal static class tcpTools
    {
        
        internal static bool genTempFile(string filename,long filesize)
        {
            try
            {
                using (var fs = new FileStream(filename, FileMode.Create))
                {
                    fs.SetLength(filesize);
                    fs.Close();
                }
                return true;
            }
            catch
            {
#if debug
                throw;
#endif
                return false;
            }

        }
        internal static long toLong(this IPEndPoint inip)
        {
            if (inip==null)
            {
                return 0;
            }
            else
            {
                return BitConverter.ToUInt32(inip.Address.GetAddressBytes(), 0) * 100000L + inip.Port;
            }
            
        }
        internal static IPEndPoint toIP(this long insz)
        {
            if (insz==0)
            {
                return null;
            }
            else
            {
                long port;
                var sz = Math.DivRem(insz, 100000, out port);
                return new IPEndPoint(new IPAddress(sz), (int)port);
            }

        }
        internal static string fileSize(this double size)
        {
            string[] units = new string[] { "B", "KB", "MB", "GB", "TB", "PB" };
            double mod = 1024.0;
            int i = 0;
            while (size >= mod)
            {
                size /= mod;
                i++;
            }
            return Math.Round(size, 2) + units[i];
        }
        internal static byte[] keepAliveValues(uint onOff, uint keepAliveTime, uint keepAliveInterval)
        {
            byte[] values = new byte[12];
            BitConverter.GetBytes(onOff).CopyTo(values, 0);
            BitConverter.GetBytes(keepAliveTime).CopyTo(values, 4);
            BitConverter.GetBytes(keepAliveInterval).CopyTo(values, 8);
            
            return values;
        }
        internal static bool isAlive(this Socket inSocket)
        {
            try
            {
                if ((inSocket.Poll(1000, SelectMode.SelectRead) && (inSocket.Available == 0)) || !inSocket.Connected)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            catch
            {
#if debug
                throw;
#endif
                return false;

            }

        }
        internal static void safeClose(this Socket inSocket)
        {
            try
            {
                inSocket.Shutdown(SocketShutdown.Both);
                inSocket.Close();
            }
            catch (Exception)
            {
#if debug
                throw;
#endif
                if (inSocket!=null)
                {
                    inSocket.Close();
                }

            }

        }
        internal static bool receiveAll(this Socket inSocket,ref byte[] tmpBin, int inlength=-1,SocketFlags inFlag=SocketFlags.None, int lmtSec=0)
        {
            try
            {
                int cd = 0;
                bool isok = false;
                var tpsec = lmtSec * 1000;
                var count = 0;
                if (inlength>tmpBin.Length||inlength==-1)
                {
                    count = tmpBin.Length;
                }
                else
                {
                    count = inlength;
                }

                if (tpsec==0)
                {
                    while (cd != count)
                    {
                        if (!inSocket.isAlive())
                        {
                            break;
                        }
                        cd += inSocket.Receive(tmpBin, cd, count - cd, inFlag);
                        if (cd == count)
                        {
                            isok = true;
                        }
                    }
                }
                else
                {
                    while (cd != count)
                    {
                        if (!inSocket.isAlive())
                        {
                            break;
                        }
                        inSocket.ReceiveTimeout = tpsec;
                        DateTime sttime = DateTime.Now;
                        cd += inSocket.Receive(tmpBin, cd, count - cd, inFlag);
                        var jgct = (DateTime.Now - sttime).Milliseconds;
                        if (cd == count)
                        {
                            isok = true;
                        }
                        else
                        {
                            tpsec = tpsec - jgct;
                            if (tpsec <= 0)
                            {
                                break;
                            }
                        }
                    }
                }


                return isok;
            }
            catch
            {
#if debug
                throw;
#endif

                return false;
            }


        }
        internal static bool sendAll(this Socket inSocket,byte[] tmpBin, int inlength=-1,SocketFlags inFlag=SocketFlags.None, int lmtSec = 0)
        {
            try
            {
                int cd = 0;
                bool isok = false;
                var tpsec = lmtSec * 1000;
                var count = 0;
                if (inlength > tmpBin.Length || inlength == -1)
                {
                    count = tmpBin.Length;
                }
                else
                {
                    count = inlength;
                }
                if (tpsec==0)
                {
                    while (cd != count)
                    {
                        if (!inSocket.isAlive())
                        {
                            break;
                        }
                        cd += inSocket.Send(tmpBin, cd, count - cd, inFlag);
                        if (cd == count)
                        {
                            isok = true;
                        }
                    }
                }
                else
                {
                    while (cd != count)
                    {
                        if (!inSocket.isAlive())
                        {
                            break;
                        }
                        inSocket.SendTimeout = tpsec;
                        DateTime sttime = DateTime.Now;
                        cd += inSocket.Send(tmpBin, cd, count - cd, inFlag);
                        var jgct = (DateTime.Now - sttime).Milliseconds;
                        if (cd == count)
                        {
                            isok = true;
                        }
                        else
                        {
                            tpsec = tpsec - jgct;
                            if (tpsec <= 0)
                            {
                                break;
                            }
                        }
                    }
                }

                return isok;


            }
            catch 
            {
#if debug
                throw;
#endif

                return false;
            }


        }
        internal static bool sendAllPack(this Socket inSocket,byte[] tmpBin)
        {
            if (inSocket.sendAll(BitConverter.GetBytes(tmpBin.LongLength)))
            {
                return inSocket.sendAll(tmpBin);
            }
            else
            {
                return false;
            }
        }
    }
    public enum fileType
    {
        receive,
        send
    } 
    public enum comType
    {
        main, p2p,obj, file
    }
    internal class comInfo
    {
        public Guid id { get; set; } = Guid.Empty;
        public int usertoken { get; set; } = -1;
        public comType cType { get; set; } = comType.obj;
        public long mainIP { get; set; } = 0;
        public fileType fType { get; set; } = fileType.send;
        public long fileSize { get; set; } = 0;
        public string fileName { get; set; }
        public byte[] bin { get; set; }
        
    }
    public delegate object netMess(IPEndPoint sender,netArg e);
    public class netArg
    {
        public bool isFile { get; set; } = false;
        public bool isOk { get; set; }
        private byte[] bin { get; set; }
        public int userToken { get; set; }
        public netArg(bool iFile, bool rOk, byte[] inbin, int inToken = -1)
        {
            isFile = iFile;
            isOk = rOk;
            bin = inbin;
            userToken = inToken;
        }
        public T getData<T>()
        {
            if (bin == null)
            {
                return default(T);
            }
            else
            {
                return bin.fromBin<T>();
            }

        }
    }


    public class fileArg : EventArgs
    {
        public IPEndPoint endp { get; set; }
        public fileType fileState { get; set; } = fileType.send;
        public string fileName { get; set; }
        public double value { get; set; }
        public string str { get; set; }
        public int userToken { get; set; }

    }
    public class infoArg : EventArgs
    {
        public bool isok { get; set; }
        public int userToken { get; set; } = -1;
        public IPEndPoint endp { get; set; }
        public infoType iType { get; set; }
        public string message { get; set; }
    }
    public enum infoType
    {
        rmt_init,
        rmt_recComplete,
        rmt_recObj,
        rmt_recFile,
        rmt_sendStart,
        rmt_sendObj,
        rmt_sendFile,
        rmt_sendComplete,
        srv_serverStart,
        srv_accept,
        srv_getReply,
        srv_sendObj,
        srv_getAllClients,
        srv_shut,
        srv_closeClient,
        srv_closeAllClients,
        cli_connect,
        cli_connectAsync,
        cli_connectComplete,
        cli_recComplete,
        cli_recObj,
        cli_sendfile,
        cli_sendStart,
        cli_sendObj,
        cli_sendComplete,
        cli_getReply,
        cli_sendPlan,
        cli_p2pAccept,
        cli_p2pRec,
        cli_getfile,
        cli_getP2pSrv,
        cli_sendObjP2P,
        cli_getReplyP2P,
        cli_sendfileP2P
    }

    public class tcpAsyncServer
    {
        private class remoteClient
        {
            private Socket workSocket=null;
            public long mainIP { get; private set; } = 0;
            public long p2pIP { get; private set; }
            private tcpAsyncServer fl;
            public IPEndPoint remoteIP { get; private set; }
            private bool ismanstop = false;
            private ConcurrentQueue<comInfo> cachesend= new ConcurrentQueue<comInfo>();
            private ReaderWriterLockSlim removewrite = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            private bool isSending = false;
            private SocketAsyncEventArgs recarg;
            private SocketAsyncEventArgs sdarg;
            
            public remoteClient(Socket insocket,tcpAsyncServer inFl)
            {
                try
                {
                    ismanstop = false;
                    fl = inFl;
                    workSocket = insocket;
                    workSocket.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                    remoteIP = workSocket.RemoteEndPoint as IPEndPoint;
                    sdarg = new SocketAsyncEventArgs();
                    sdarg.Completed += Sdarg_Completed;
                    recarg = new SocketAsyncEventArgs();
                    recarg.SetBuffer(new byte[8], 0, 8);
                    recarg.Completed += Recarg_Completed;
                    workSocket.ReceiveAsync(recarg);
                }
                catch (Exception ex)
                {

#if debug
                    throw;
#endif
                    close();
                    if (!ismanstop&&mainIP==0)
                    {
                        callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_init,message=ex.Message });
                    }
                    callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_init,message=ex.Message });
                }

            }
            private void callEvent<T>(EventHandler<T> inevt,T inarg,bool intask = false) where T:EventArgs
            {
                try
                {
                    if (intask)
                    {
                        if (inevt != null)
                        {
                            Task.Factory.StartNew(() =>
                            {
                                inevt(fl, inarg);
                            });
                        }

                    }
                    else
                    {
                        inevt?.Invoke(fl, inarg);
                    }
                }
                catch
                {
#if debug
                    throw;
#endif
                    return;
                }
            }
            private void Recarg_Completed(object sender, SocketAsyncEventArgs e)
            {
                try
                {
                    if (!workSocket.isAlive())
                    {
                        close();
                        if (!ismanstop&&mainIP==0)
                        {
                            callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_recComplete });
                        }
                    }
                    else
                    {
                        if (e.SocketError==SocketError.Success&&e.BytesTransferred==8)
                        {
                            var length = BitConverter.ToInt64(e.Buffer, 0);
                            var cache = new byte[length];
                            if (workSocket.receiveAll(ref cache))
                            {
                                var tmp = cache.fromBin<comInfo>();
                                switch (tmp.cType)
                                {
                                    case comType.main:
                                        if (tmp.mainIP == 0)
                                        {
                                            p2pIP = (new IPEndPoint(remoteIP.Address, tmp.bin.fromBin<int>())).toLong() ;
                                            callEvent(fl.clientIn, new infoArg { endp = remoteIP, iType = infoType.rmt_recObj }, true);
                                        }
                                        mainIP = tmp.mainIP;
                                        break;
                                    case comType.p2p:
                                        if (fl.cusers.TryGetValue(tmp.mainIP,out var declient))
                                        {
                                            tmp.mainIP = declient.p2pIP;
                                            send(tmp);
                                        }
                                        else
                                        {
                                            tmp.mainIP = 0;
                                            send(tmp);
                                        }
                                        break;
                                    case comType.obj:
                                        if (tmp.id == Guid.Empty)
                                        {
                                            if (fl.recData!=null)
                                            {
                                                Task.Factory.StartNew(()=> {
                                                    fl.recData.Invoke(remoteIP, new netArg(false, true, tmp.bin, tmp.usertoken));
                                                });
                                            }

                                        }
                                        else
                                        {
                                            if (fl.replyCache.TryGetValue(tmp.id, out var tpifo))
                                            {
                                                tpifo.bin = tmp.bin;
                                                tpifo.autoEvent.Set();
                                            }
                                            else
                                            {
                                                if (fl.recData != null)
                                                {
                                                    Task.Factory.StartNew(() =>
                                                    {
                                                        var jg= fl.recData.Invoke(remoteIP, new netArg(false, true, tmp.bin, tmp.usertoken));
                                                        send(new comInfo { id = tmp.id, bin = jg.toBin(), cType = comType.obj, usertoken = tmp.usertoken });
                                                    });
                                                }
                                            }
                                        }
                                        break;
                                    case comType.file:
                                        switch (tmp.fType)
                                        {
                                            case fileType.receive:
                                                send(tmp);
                                                break;
                                            case fileType.send:
                                                var realfile = fl.tempDirectory + Path.GetFileName(tmp.fileName);
                                                var tmpfile = realfile + ".tmp";
                                                if (File.Exists(tmpfile) || File.Exists(realfile))
                                                {
                                                    realfile = fl.tempDirectory + Path.GetFileNameWithoutExtension(tmp.fileName) + "_" + DateTime.Now.ToString("yyyyMMddHHmmss") + Path.GetExtension(tmp.fileName);
                                                    tmpfile = realfile + ".tmp";
                                                }
                                                if (tcpTools.genTempFile(tmpfile, tmp.fileSize))
                                                {
                                                    workSocket.Send(new byte[] { 1 });
                                                    long cd = 0;
                                                    using (var mf = MemoryMappedFile.CreateFromFile(tmpfile, FileMode.Open))
                                                    {
                                                        using (var mvs = mf.CreateViewAccessor())
                                                        {
                                                            byte[] filebuff = new byte[fl.txBuff];
                                                            long rest = 0;
                                                            while (cd < tmp.fileSize)
                                                            {
                                                                if (!workSocket.isAlive())
                                                                {
                                                                    break;
                                                                }
                                                                rest = tmp.fileSize - cd;
                                                                if (rest <= fl.txBuff)
                                                                {
                                                                    if (!workSocket.receiveAll(ref filebuff, (int)rest))
                                                                    {
                                                                        break;
                                                                    }
                                                                    mvs.WriteArray(cd, filebuff, 0, (int)rest);
                                                                    cd += rest;
                                                                }
                                                                else
                                                                {
                                                                    if (!workSocket.receiveAll(ref filebuff))
                                                                    {
                                                                        break;
                                                                    }
                                                                    mvs.WriteArray(cd, filebuff, 0, fl.txBuff);
                                                                    cd += fl.txBuff;
                                                                }
                                                            }

                                                        }
                                                    }
                                                    if (cd != tmp.fileSize)
                                                    {
                                                        Thread.Sleep(500);
                                                        File.Delete(tmpfile);
                                                        fl.recData?.Invoke(mainIP == 0 ? remoteIP : mainIP.toIP(), new netArg(true, false, tmp.fileName.toBin(), tmp.usertoken));
                                                    }
                                                    else
                                                    {
                                                        File.Move(tmpfile, realfile);
                                                        fl.recData?.Invoke(mainIP == 0 ? remoteIP : mainIP.toIP(), new netArg( true, true, realfile.toBin(), tmp.usertoken));
                                                    }
                                                }
                                                else
                                                {
                                                    workSocket.Send(new byte[] { 0 });
                                                }
                                                break;
                                            default:
                                                break;
                                        }

                                        break;
                                    default:
                                        break;
                                }

                            }
                        }
                        workSocket.ReceiveAsync(e);
                    }
                }
                catch (Exception ex)
                {
#if debug
                    throw;
#endif
                    close();
                    if (!ismanstop)
                    {
                        if (mainIP == 0)
                        {
                            callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_recObj,message=ex.Message });
                            callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_recObj ,message=ex.Message});
                        }
                        else
                        {
                            callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_recFile ,message=ex.Message});
                        }

                    }
                    
                }
            }
            public void close(bool ownstop=false)
            {
                try
                {
                    if (ownstop)
                    {
                        removewrite.EnterWriteLock();
                        ismanstop = ownstop;
                        removewrite.ExitWriteLock();
                    }

                    workSocket.safeClose();
                    if (fl.cusers != null)
                    {
                        if (fl.cusers.ContainsKey(this.remoteIP.toLong()))
                        {
                            remoteClient tmp;
                            fl.cusers.TryRemove(this.remoteIP.toLong(), out tmp);
                        }
                        var fileSkt= fl.cusers.FirstOrDefault(n => n.Value.mainIP == this.remoteIP.toLong());
                        if (fileSkt.Key!=0)
                        {
                            fileSkt.Value.close(ownstop);
                        }
                    }

                }
                catch (Exception)
                {
#if debug
                    throw;
#endif
                    return;
                }
            }

            public void send(comInfo tmpData,bool canwait=true)
            {
                try
                {
                    if (!workSocket.isAlive())
                    {
                        close();
                        if (!ismanstop && mainIP == 0)
                        {
                            callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_sendStart });
                        }
                    }
                    else if (isSending&&canwait)
                    {
                        cachesend.Enqueue(tmpData);
                    }
                    else
                    {
                        if (canwait)
                        {
                            isSending = true;
                        }
                        switch (tmpData.cType)
                        {
                            case comType.p2p:
                            case comType.obj:
                                sdarg.UserToken = tmpData.usertoken;
                                var buffer = tmpData.toBin();
                                var tpack = new SendPacketsElement[2];
                                tpack[0] = new SendPacketsElement(BitConverter.GetBytes(buffer.LongLength));
                                tpack[1] = new SendPacketsElement(buffer);
                                sdarg.SendPacketsElements = tpack;
                                workSocket.SendPacketsAsync(sdarg);
                                break;
                            case comType.file:
                                if (!File.Exists(tmpData.fileName))
                                {
                                    workSocket.Send(BitConverter.GetBytes(-1L));
                                }
                                else
                                {
                                    FileInfo ifo = new FileInfo(tmpData.fileName);
                                    workSocket.Send(BitConverter.GetBytes(ifo.Length));
                                    using (var mf = MemoryMappedFile.CreateFromFile(tmpData.fileName, FileMode.Open))
                                    {
                                        using (var mvs = mf.CreateViewAccessor())
                                        {
                                            long cd = 0;

                                            byte[] filebuff = new byte[fl.txBuff];
                                            long rest = 0;
                                            while (cd < ifo.Length)
                                            {
                                                if (!workSocket.isAlive())
                                                {
                                                    break;
                                                }
                                                rest = ifo.Length - cd;
                                                if (rest <= fl.txBuff)
                                                {
                                                    mvs.ReadArray(cd, filebuff, 0, (int)rest);
                                                    if (!workSocket.sendAll(filebuff, (int)rest))
                                                    {
                                                        break;
                                                    }
                                                    cd += rest;
                                                }
                                                else
                                                {
                                                    mvs.ReadArray(cd, filebuff, 0, fl.txBuff);
                                                    if (!workSocket.sendAll(filebuff))
                                                    {
                                                        break;
                                                    }
                                                    cd += fl.txBuff;
                                                }
                                                if (fl.fileDelay > 0)
                                                {
                                                    Thread.Sleep(fl.fileDelay);
                                                }

                                            }
                                        }
                                    }



                                }
                                break;
                            default:
                                break;
                        }

                    }
                    
                }
                catch (Exception ex)
                {
#if debug
                    throw;
#endif
                    close();
                    if (!ismanstop)
                    {
                        if (mainIP == 0)
                        {
                            callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_sendObj, message = ex.Message });
                            callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_sendObj, message = ex.Message });
                        }
                        else
                        {
                            callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_sendObj, message = ex.Message });
                        }

                    }

                }
            }

            private void Sdarg_Completed(object sender, SocketAsyncEventArgs e)
            {
                try
                {
                    if (e.SocketError==SocketError.Success)
                    {
                        callEvent(fl.sendOver,new infoArg { endp=remoteIP,isok=true,userToken=(int)e.UserToken,iType=infoType.rmt_sendComplete}, true);
                    }
                    else
                    {
                        callEvent(fl.sendOver, new infoArg { endp = remoteIP, isok = false, userToken = (int)e.UserToken, iType = infoType.rmt_sendComplete }, true);
                    }
                    if (cachesend.Count > 0)
                    {
                        if (cachesend.TryDequeue(out var tmp))
                        {
                            send(tmp,false);
                        }
                        else
                        {
                            isSending = false;
                        }
                    }
                    else
                    {
                        isSending = false;

                    }
                }
                catch (Exception ex)
                {
#if debug
                    throw;
#endif
                    close();
                    if (!ismanstop)
                    {
                        callEvent(fl.clientOut, new infoArg { endp = remoteIP, iType = infoType.rmt_sendComplete, message = ex.Message });
                        callEvent(fl.errorRec, new infoArg { endp = remoteIP, iType = infoType.rmt_sendComplete, message = ex.Message });
                    }
                }
            }


        }
        private Socket server = null;
        private ConcurrentDictionary<long, remoteClient> cusers = new ConcurrentDictionary<long, remoteClient>();
        public event EventHandler<infoArg> clientIn;
        public event EventHandler<infoArg> clientOut;
        public event netMess recData;
        public event EventHandler<infoArg> sendOver;
        public event EventHandler<infoArg> unexpectedStop;
        public event EventHandler<infoArg> errorRec;
        private ConcurrentDictionary<Guid, replyIfo> replyCache;
        private int serverPort = 0;
        private bool ownClose = false;
        private bool isrun = false;
        private ReaderWriterLockSlim runlock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        public int txBuff { get; private set; }
        public string tempDirectory { get; set; }
        public bool available
        {
            get
            {
                if (server != null && isrun)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

        }
        public int fileDelay { get; set; }
        public tcpAsyncServer(int insevport, int sendFileDelay=0,int fileCache = 4*1024*1024)
        {
            serverPort = insevport;
            txBuff = fileCache;
            fileDelay = sendFileDelay;
            tempDirectory = AppDomain.CurrentDomain.BaseDirectory+"downTMP\\";
            if (!Directory.Exists(tempDirectory))
            {
                Directory.CreateDirectory(tempDirectory);
            }
        }
        private void callEvent<T>(EventHandler<T> inevt, T inarg, bool intask = false) where T : EventArgs
        {
            try
            {
                if (intask)
                {
                    if (inevt != null)
                    {
                        Task.Factory.StartNew(() =>
                        {
                            inevt(this, inarg);
                        });
                    }

                }
                else
                {
                    inevt?.Invoke(this, inarg);
                }
            }
            catch
            {
#if debug
                throw;
#endif
                return;
            }
        }

        public void start()
        {
            try
            {
                if (server != null && isrun)
                {
                    return;
                }
                replyCache = new ConcurrentDictionary<Guid, replyIfo>();
                ownClose = false;
                server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                server.Bind(new IPEndPoint(IPAddress.Any, serverPort));
                server.Listen(10);
                var acptArg = new SocketAsyncEventArgs();
                acptArg.Completed += acptArg_Completed;
                server.AcceptAsync(acptArg);
                runlock.EnterWriteLock();
                isrun = true;
                runlock.ExitWriteLock();
            }
            catch(Exception ex)
            {
#if debug
                    throw;
#endif
                runlock.EnterWriteLock();
                isrun = false;
                runlock.ExitWriteLock();
                callEvent(unexpectedStop, new infoArg { iType = infoType.srv_serverStart, message = ex.Message });
                callEvent(errorRec, new infoArg { iType = infoType.srv_serverStart, message = ex.Message });
            }


        }
        void acptArg_Completed(object sender, SocketAsyncEventArgs e)
        {

            try
            {
                var tpskt = e.AcceptSocket;
                e.AcceptSocket = null;
                server.AcceptAsync(e);
                if (tpskt!=null&&tpskt.RemoteEndPoint!=null)
                {

                    cusers.TryAdd((tpskt.RemoteEndPoint as IPEndPoint).toLong(), new remoteClient(tpskt,this));

                }
                else if (tpskt!=null && tpskt.RemoteEndPoint==null)
                {
                    tpskt.safeClose();
                }


            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif

                shut();
                runlock.EnterWriteLock();
                isrun = false;
                runlock.ExitWriteLock();
                
                if (!ownClose)
                {
                    callEvent(unexpectedStop, new infoArg { iType = infoType.srv_accept, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.srv_accept, message = ex.Message });
                }
            }
        }
        public T getReply<T>(IPEndPoint deip, object inObj, int inusertoken = -1, int timeout=60000)
        {
            try
            {
                if (isrun)
                {
                    if (cusers.TryGetValue(deip.toLong(), out var sdsk))
                    {
                        if (timeout <= 0)
                        {
                            timeout = 1;
                        }
                        var objid = Guid.NewGuid();
                        var tifo = new replyIfo();
                        if (replyCache.TryAdd(objid, tifo))
                        {
                            var fsdata = new comInfo { cType=comType.obj, bin = inObj.toBin(), id = objid, usertoken = inusertoken };

                            sdsk.send(fsdata);
                            if (tifo.autoEvent.WaitOne(timeout))
                            {
                                if (tifo.bin!=null)
                                {
                                    var ret = tifo.bin.fromBin<T>();

                                    replyCache.TryRemove(objid, out var tmp);
                                    return ret;
                                }
                                else
                                {
                                    replyCache.TryRemove(objid, out var tmp);
                                    return default(T);
                                }

                            }
                            else
                            {
                                replyCache.TryRemove(objid, out var tmp);
                                return default(T);
                            }
                        }
                        else
                        {
                            var fsdata = new comInfo { cType = comType.obj, bin = inObj.toBin(), id = Guid.Empty, usertoken = inusertoken };

                            sdsk.send(fsdata);
                            return default(T);
                        }

                    }
                    else
                    {
                        return default(T);
                    }
                }
                else
                {
                    return default(T);
                }



            }
            catch(Exception ex)
            {

#if debug
                throw;
#endif
                shut();
                runlock.EnterWriteLock();
                isrun = false;
                runlock.ExitWriteLock();
                if (!ownClose)
                {
                    callEvent(unexpectedStop, new infoArg { iType = infoType.srv_getReply, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.srv_getReply, message = ex.Message });
                }
                return default(T);
            }
        }
        public void sendObject(IPEndPoint deip,object inObj,int inusertoken = -1)
        {
            try
            {

                if (isrun)
                {
                    if (cusers.TryGetValue(deip.toLong(), out var sdsk))
                    {
                        var fsdata = new comInfo { cType = comType.obj, bin = inObj.toBin(), id = Guid.Empty, usertoken = inusertoken };
                        sdsk.send(fsdata);
                    }
                }



            }
            catch(Exception ex)
            {

#if debug
                throw;
#endif
                shut();
                runlock.EnterWriteLock();
                isrun = false;
                runlock.ExitWriteLock();
                if (!ownClose)
                {
                    callEvent(unexpectedStop, new infoArg { iType = infoType.srv_sendObj, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.srv_sendObj, message = ex.Message });
                }
            }
        }
        public List<IPEndPoint> getAllClients()
        {
            try
            {
                return cusers.Where(n => n.Value.mainIP == 0).Select(m => m.Value.remoteIP).ToList();
            }
            catch(Exception ex)
            {
#if debug
                throw;
#endif

                if (!ownClose)
                {
                    callEvent(errorRec, new infoArg { iType = infoType.srv_getAllClients, message = ex.Message });
                }
                return new List<IPEndPoint>();
            }

        }
        private void shut()
        {
            try
            {
                foreach (var item in cusers.Values)
                {
                    item.close(true);
                }
                cusers.Clear();
                server.safeClose();
            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif

                cusers = new ConcurrentDictionary<long, remoteClient>();
                if (server!=null)
                {
                    server.safeClose();
                }
                callEvent(errorRec, new infoArg { iType = infoType.srv_shut, message = ex.Message });
            }

        }
        public void stop()
        {
            ownClose = true;
            runlock.EnterWriteLock();
            isrun = false;
            runlock.ExitWriteLock();
            shut();
        }
        public void closeClient(IPEndPoint deIP)
        {
            try
            {
                if (isrun)
                {
                    if (cusers.TryRemove(deIP.toLong(), out var sdsk))
                    {
                        sdsk.close(true);
                    }
                }


            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif
                if (cusers.TryRemove(deIP.toLong(), out var sdsk))
                {
                    sdsk.close(true);
                }
                callEvent(errorRec, new infoArg { iType = infoType.srv_closeClient, message = ex.Message });
            }


        }
        public void closeAllClients()
        {

            try
            {
                if (isrun)
                {
                    foreach (var item in cusers.Values)
                    {
                        item.close(true);
                    }
                    cusers.Clear();
                }

            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif
                shut();
                runlock.EnterWriteLock();
                isrun = false;
                runlock.ExitWriteLock();
                if (!ownClose)
                {
                    callEvent(unexpectedStop, new infoArg { iType = infoType.srv_closeAllClients, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.srv_closeAllClients, message = ex.Message });
                }
            }
        }

    }
    public class tcpAsyncClient
    {
        private Socket workSocket = null;
        private Socket p2pSocket = null;
        private ConcurrentDictionary<long, Socket> sktLst = new ConcurrentDictionary<long, Socket>();
        public int txBuff { get;private set; }
        public event EventHandler<infoArg> connectResult;
        public event EventHandler<infoArg> disConnected;
        public event netMess recData;
        public event EventHandler<fileArg> recFileRate;
        public event EventHandler<fileArg> sendFileRate;
        public event EventHandler<fileArg> recFileSpeed;
        public event EventHandler<fileArg> sendFileSpeed;
        public event EventHandler<infoArg> sendOver;
        public event EventHandler<infoArg> errorRec;
        public IPEndPoint lcEDP { get; private set; } = null;
        private ReaderWriterLockSlim iplock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private bool ownClose = false;
        private ConcurrentQueue<comInfo> cachesend = new ConcurrentQueue<comInfo>();
        private ConcurrentDictionary<Guid, replyIfo> replyCache;
        private bool isSending = false;
        private SocketAsyncEventArgs recarg;
        private SocketAsyncEventArgs sdarg;
        private SocketAsyncEventArgs cntarg;
        private IPEndPoint srvhost;
        public string tempDirectory { get; set; }
        public bool iscnting { get; private set; } = false;
        public bool available
        {
            get
            {
                if (workSocket!=null && lcEDP!=null&&workSocket.Connected&&workSocket.isAlive())
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

        }
        public int fileDelay { get; set; }
        public tcpAsyncClient(int sendFileDelay=0,int fileCache = 4 * 1024 * 1024)
        {
            fileDelay = sendFileDelay;
            txBuff = fileCache;
            tempDirectory = AppDomain.CurrentDomain.BaseDirectory + "downTMP\\";
            if (!Directory.Exists(tempDirectory))
            {
                Directory.CreateDirectory(tempDirectory);
            }
            cntarg = new SocketAsyncEventArgs();
            cntarg.Completed += Cntarg_Completed;
            recarg = new SocketAsyncEventArgs();
            recarg.SetBuffer(new byte[8], 0, 8);
            recarg.Completed += Recarg_Completed;
            sdarg = new SocketAsyncEventArgs();
            sdarg.Completed += Sdarg_Completed;
        }
        private void callEvent<T>(EventHandler<T> inevt, T inarg, bool intask = false) where T : EventArgs
        {
            try
            {
                if (intask)
                {
                    if (inevt != null)
                    {
                        Task.Factory.StartNew(() =>
                        {
                            inevt(this, inarg);
                        });
                    }

                }
                else
                {
                    inevt?.Invoke(this, inarg);
                }
            }
            catch
            {
#if debug
                throw;
#endif
                return;
            }
        }
        public bool connect(IPEndPoint deip)
        {
            if (available || iscnting)
            {
                return false;
            }
            iscnting = true;
            try
            {
                replyCache = new ConcurrentDictionary<Guid, replyIfo>();
                sktLst = new ConcurrentDictionary<long, Socket>();
                srvhost = deip;
                if (p2pSocket!=null)
                {
                    p2pSocket.safeClose();
                }
                p2pSocket= new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                p2pSocket.Bind(new IPEndPoint(IPAddress.Any, 0));
                p2pSocket.Listen(10);
                var p2pAcptArg= new SocketAsyncEventArgs();
                p2pAcptArg.Completed += P2pAcptArg_Completed;
                p2pSocket.AcceptAsync(p2pAcptArg);
                workSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                workSocket.Connect(deip);
                if (workSocket.isAlive())
                {
                    iplock.EnterWriteLock();
                    lcEDP = workSocket.LocalEndPoint as IPEndPoint;
                    iplock.ExitWriteLock();
                    workSocket.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                    var buffer = (new comInfo { cType = comType.main,bin=(p2pSocket.LocalEndPoint as IPEndPoint).Port.toBin() }).toBin();
                    var tp = buffer.fromBin<comInfo>();
                    workSocket.sendAllPack(buffer);
                    workSocket.ReceiveAsync(recarg);
                    return true;
                }
                else
                {
                    close();
                    return false;
                }
            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif
                close();
                callEvent(errorRec, new infoArg { iType = infoType.cli_connect, message = ex.Message });
                return false;
            }
            finally
            {
                iscnting = false;
            }
           

        }

        public void connectAsync(IPEndPoint deip)
        {
            try
            {
                if (available || iscnting)
                {
                    return;
                }
                iscnting = true;
                iplock.EnterWriteLock();
                lcEDP = null;
                iplock.ExitWriteLock();
                ownClose = false;
                replyCache = new ConcurrentDictionary<Guid, replyIfo>();
                srvhost = deip;
                if (p2pSocket!=null)
                {
                    p2pSocket.safeClose();
                }
                p2pSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                p2pSocket.Bind(new IPEndPoint(IPAddress.Any, 0));
                p2pSocket.Listen(10);
                var p2pAcptArg = new SocketAsyncEventArgs();
                p2pAcptArg.Completed += P2pAcptArg_Completed;
                p2pSocket.AcceptAsync(p2pAcptArg);
                cntarg.RemoteEndPoint = srvhost;
                workSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                workSocket.ConnectAsync(cntarg);
            }
            catch(Exception ex)
            {
#if debug
                    throw;
#endif
                close();
                callEvent(connectResult, new infoArg { endp=srvhost,isok=false,iType=infoType.cli_connectAsync});
                callEvent(errorRec, new infoArg { iType = infoType.cli_connectAsync, message = ex.Message });
                iscnting = false;
                
            }

        }
        private void P2pAcptArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var tpskt = e.AcceptSocket;
            try
            {
                
                e.AcceptSocket = null;
                p2pSocket.AcceptAsync(e);
                if (tpskt != null && tpskt.RemoteEndPoint != null)
                {
                    sktLst.TryAdd(((IPEndPoint)tpskt.RemoteEndPoint).toLong(), tpskt);
                    tpskt.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                    var p2precarg = new SocketAsyncEventArgs();
                    p2precarg.SetBuffer(new byte[8], 0, 8);
                    p2precarg.Completed += P2precarg_Completed;
                    tpskt.ReceiveAsync(p2precarg);
                }

            }
            catch (Exception ex)
            {

                sktLst.TryRemove(((IPEndPoint)tpskt.RemoteEndPoint).toLong(), out var tp);
                if (tpskt != null)
                {
                    tpskt.safeClose();
                }
                if (p2pSocket != null)
                {
                    p2pSocket.safeClose();
                }
                callEvent(errorRec, new infoArg { iType = infoType.cli_p2pAccept, message = ex.Message });

            }
        }

        private void P2precarg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var tpskt = (Socket)sender;
            try
            {

                if (tpskt.isAlive())
                {
                    if (e.SocketError == SocketError.Success && e.BytesTransferred == 8)
                    {
                        var length = BitConverter.ToInt64(e.Buffer, 0);
                        var cache = new byte[length];
                        if (tpskt.receiveAll(ref cache))
                        {
                            var tmp = cache.fromBin<comInfo>();
                            var rmtIP = tmp.mainIP.toIP();
                            switch (tmp.cType)
                            {

                                case comType.obj:
                                    if (recData!=null)
                                    {
                                        var jg = recData.Invoke(rmtIP, new netArg( false, true, tmp.bin, tmp.usertoken));
                                        if (tmp.id!=Guid.Empty)
                                        {
                                            tpskt.sendAllPack(new comInfo { id = tmp.id, bin = jg.toBin(), cType = comType.obj, usertoken = tmp.usertoken }.toBin());
                                        }
                                        
                                    }
                                    break;
                                case comType.file:
                                    var realfile = tempDirectory + Path.GetFileName(tmp.fileName);
                                    var tmpfile = realfile + ".tmp";
                                    if (File.Exists(tmpfile) || File.Exists(realfile))
                                    {
                                        realfile = tempDirectory + Path.GetFileNameWithoutExtension(tmp.fileName) + "_" + DateTime.Now.ToString("yyyyMMddHHmmss") + Path.GetExtension(tmp.fileName);
                                        tmpfile = realfile + ".tmp";
                                    }
                                    if (tcpTools.genTempFile(tmpfile, tmp.fileSize))
                                    {
                                        tpskt.Send(new byte[] { 1 });
                                        long cd = 0;
                                        using (var mf = MemoryMappedFile.CreateFromFile(tmpfile, FileMode.Open))
                                        {
                                            using (var mvs = mf.CreateViewAccessor())
                                            {
                                                byte[] filebuff = new byte[txBuff];
                                                Stopwatch stp = new Stopwatch();
                                                long spd = 0;
                                                long rest = 0;
                                                stp.Start();
                                                while (cd < tmp.fileSize)
                                                {
                                                    if (!tpskt.isAlive())
                                                    {
                                                        break;
                                                    }
                                                    rest = tmp.fileSize - cd;

                                                    if (rest <= txBuff)
                                                    {
                                                        if (!tpskt.receiveAll(ref filebuff, (int)rest))
                                                        {
                                                            break;
                                                        }
                                                        mvs.WriteArray(cd, filebuff, 0, (int)rest);
                                                        cd += rest;
                                                    }
                                                    else
                                                    {
                                                        if (!tpskt.receiveAll(ref filebuff))
                                                        {
                                                            break;
                                                        }
                                                        mvs.WriteArray(cd, filebuff, 0, txBuff);
                                                        cd += txBuff;
                                                    }
                                                    if (recFileRate != null)
                                                    {
                                                        var dt = cd;
                                                        Task.Factory.StartNew(() =>
                                                        {
                                                            var rate = dt / (double)tmp.fileSize;
                                                            recFileRate(this, new fileArg { endp = rmtIP, fileName = realfile, fileState = fileType.receive, str = rate.ToString("p2"), value = rate, userToken = tmp.usertoken });
                                                        });
                                                    }

                                                    if (recFileSpeed != null)
                                                    {
                                                        if (spd == 0 || (spd > 0 && stp.Elapsed.TotalSeconds >= 1))
                                                        {
                                                            var dt = cd - spd;
                                                            spd = cd;
                                                            var sj = stp.Elapsed.TotalSeconds;
                                                            Task.Factory.StartNew(() =>
                                                            {
                                                                var speed = dt / sj;
                                                                recFileSpeed(this, new fileArg { endp = rmtIP, fileName = realfile, fileState = fileType.receive, str = speed.fileSize() + "/s", value = speed, userToken = tmp.usertoken });
                                                            });
                                                            stp.Restart();
                                                        }

                                                    }
                                                }
                                                stp.Stop();

                                            }
                                        }
                                        if (cd != tmp.fileSize)
                                        {
                                            Thread.Sleep(500);
                                            File.Delete(tmpfile);
                                            recData?.Invoke(rmtIP, new netArg( true, false, tmp.fileName.toBin(), tmp.usertoken));
                                        }
                                        else
                                        {
                                            File.Move(tmpfile, realfile);
                                            recData?.Invoke(rmtIP, new netArg( true, true, realfile.toBin(), tmp.usertoken));
                                        }

                                    }
                                    else
                                    {
                                        tpskt.Send(new byte[] { 0 });
                                        recData?.Invoke(rmtIP, new netArg( true, false, tmp.fileName.toBin(), tmp.usertoken));
                                    }

                                    break;
                                default:
                                    break;
                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (p2pSocket != null)
                {
                    p2pSocket.safeClose();
                }
                callEvent(errorRec, new infoArg { iType = infoType.cli_p2pRec, message = ex.Message });

            }
            finally
            {
                sktLst.TryRemove(((IPEndPoint)tpskt.RemoteEndPoint).toLong(), out var tp);
                if (tpskt != null)
                {
                    tpskt.safeClose();
                }
                

            }
        }

        private void Cntarg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                if (e.ConnectSocket != null)
                {
                    iplock.EnterWriteLock();
                    lcEDP = workSocket.LocalEndPoint as IPEndPoint;
                    iplock.ExitWriteLock();
                    workSocket.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                    var buffer = (new comInfo { cType = comType.main,bin= (p2pSocket.LocalEndPoint as IPEndPoint).Port.toBin() }).toBin();
                    workSocket.sendAllPack(buffer);
                    callEvent(connectResult, new infoArg { endp = srvhost, isok = true, iType = infoType.cli_connectComplete }, true);
                    workSocket.ReceiveAsync(recarg);
                }
                else
                {
                    
                    close();
                    callEvent(connectResult, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_connectComplete });
                }
            }
            catch (Exception ex)
            {
#if debug
                throw;
#endif
                close();
                
                if (!ownClose)
                {
                    callEvent(connectResult, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_connectComplete });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_connectComplete, message = ex.Message });
                }
                
            }
            finally
            {
                iscnting = false;
            }

        }

        private void Recarg_Completed(object sender, SocketAsyncEventArgs e)
        {

            try
            {
                if (!workSocket.isAlive())
                {
                    close();
                    
                    if (!ownClose)
                    {
                        callEvent(disConnected, new infoArg { endp = srvhost, iType = infoType.cli_recComplete });
                    }
                }
                else
                {
                    if (e.SocketError == SocketError.Success && e.BytesTransferred == 8)
                    {
                        var length = BitConverter.ToInt64(e.Buffer, 0);
                        var cache = new byte[length];
                        if (workSocket.receiveAll(ref cache))
                        {
                            var tmp = cache.fromBin<comInfo>();

                            if (tmp.id == Guid.Empty)
                            {
                                if (recData!=null)
                                {
                                    Task.Factory.StartNew(()=>recData.Invoke(srvhost, new netArg( false, true, tmp.bin, tmp.usertoken)));
                                }
                            }
                            else
                            {
                                if (replyCache.TryGetValue(tmp.id, out var tpifo))
                                {
                                    if (tmp.cType==comType.p2p)
                                    {
                                        if (tmp.mainIP!=0)
                                        {
                                            tpifo.bin = tmp.mainIP.toBin();
                                        }
                                        else
                                        {
                                            tpifo.bin = null;
                                        }
                                        
                                    }
                                    else
                                    {
                                        tpifo.bin = tmp.bin;
                                    }
                                    
                                    tpifo.autoEvent.Set();
                                }
                                else
                                {
                                    if (recData != null)
                                    {
                                        Task.Factory.StartNew(() =>
                                        {
                                            var jg = recData.Invoke(srvhost, new netArg( false, true, tmp.bin, tmp.usertoken));
                                            send(new comInfo { id = tmp.id, bin = jg.toBin(), cType = comType.obj, usertoken = tmp.usertoken });
                                        });
                                    }
                                }
                            }

                        }
                    }
                    workSocket.ReceiveAsync(e);
                }
            }
            catch(Exception ex)
            {
#if debug
                throw;
#endif
                close();
                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_recObj, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_recObj, message = ex.Message });
                }

            }
        }
        public void sendFile(string filename,int inusertoken=-1)
        {
            if (available && File.Exists(filename))
            {
                Task.Factory.StartNew(()=> {
                    Socket fileSocket = null;
                    try
                    {

                        fileSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        fileSocket.Connect(srvhost);
                        if (fileSocket.isAlive())
                        {
                            sktLst.TryAdd(((IPEndPoint)fileSocket.LocalEndPoint).toLong(), fileSocket);
                            fileSocket.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                            var buffer = (new comInfo { cType = comType.main, mainIP = lcEDP.toLong() }).toBin();
                            fileSocket.sendAllPack(buffer);

                            var ifo = new FileInfo(filename);
                            var tpIfo = new comInfo { fileName = filename, fileSize = ifo.Length, fType = fileType.send, cType = comType.file, usertoken = inusertoken };
                            fileSocket.sendAllPack(tpIfo.toBin());
                            var sdbf = new byte[1];
                            if (fileSocket.receiveAll(ref sdbf))
                            {
                                if (sdbf[0] == 1)
                                {
                                    using (var mf = MemoryMappedFile.CreateFromFile(filename, FileMode.Open))
                                    {
                                        using (var mvs = mf.CreateViewAccessor())
                                        {
                                            long cd = 0;
                                            long rest = 0;
                                            Stopwatch stp = new Stopwatch();
                                            long spd = 0;
                                            byte[] filebuff = new byte[txBuff];
                                            stp.Start();
                                            while (cd < ifo.Length)
                                            {
                                                if (!fileSocket.isAlive())
                                                {
                                                    break;
                                                }
                                                rest = ifo.Length - cd;
                                                if (rest <= txBuff)
                                                {

                                                    mvs.ReadArray(cd, filebuff, 0, (int)rest);
                                                    if (!fileSocket.sendAll(filebuff, (int)rest))
                                                    {
                                                        break;
                                                    }
                                                    cd += rest;
                                                }
                                                else
                                                {
                                                    mvs.ReadArray(cd, filebuff, 0, txBuff);
                                                    if (!fileSocket.sendAll(filebuff))
                                                    {
                                                        break;
                                                    }
                                                    cd += txBuff;
                                                }
                                                if (sendFileRate != null)
                                                {
                                                    var dt = cd;
                                                    Task.Factory.StartNew(() =>
                                                    {
                                                        var rate = dt / (double)ifo.Length;
                                                        sendFileRate(this, new fileArg { endp = srvhost, fileName = filename, fileState = fileType.send, str = rate.ToString("p2"), value = rate, userToken = inusertoken });
                                                    });
                                                }
                                                if (sendFileSpeed != null)
                                                {
                                                    if (spd == 0 || (spd > 0 && stp.Elapsed.TotalSeconds >= 1))
                                                    {
                                                        var dt = cd - spd;
                                                        spd = cd;
                                                        var sj = stp.Elapsed.TotalSeconds;
                                                        Task.Factory.StartNew(() =>
                                                        {
                                                            var speed = dt / sj;
                                                            sendFileSpeed(this, new fileArg { endp = srvhost, fileName = filename, fileState = fileType.send, str = speed.fileSize() + "/s", value = speed, userToken = inusertoken });
                                                        });
                                                        stp.Restart();
                                                    }
                                                }
                                                if (fileDelay > 0)
                                                {
                                                    Thread.Sleep(fileDelay);
                                                }

                                            }
                                            stp.Stop();
                                            if (cd != ifo.Length)
                                            {
                                                callEvent(sendOver, new infoArg { endp=srvhost,isok=false,iType= infoType.cli_sendfile ,message=filename,userToken=inusertoken});
                                            }
                                            else
                                            {
                                                callEvent(sendOver, new infoArg { endp = srvhost, isok = true, iType = infoType.cli_sendfile, message = filename, userToken = inusertoken });
                                            }
                                        }
                                    }


                                }
                                else
                                {
                                    callEvent(sendOver, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_sendfile, message = filename, userToken = inusertoken });
                                }
                            }
                            else
                            {
                                callEvent(sendOver, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_sendfile, message = filename, userToken = inusertoken });
                            }
                        }
                        else
                        {
                            callEvent(sendOver, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_sendfile, message = filename, userToken = inusertoken });
                        }

                    }
                    catch (Exception ex)
                    {
                        callEvent(sendOver, new infoArg { endp = srvhost, isok = false, iType = infoType.cli_sendfile, message = filename, userToken = inusertoken });
                        if (!ownClose)
                        {
                            callEvent(errorRec, new infoArg { iType = infoType.cli_sendfile, message = ex.Message });
                        }
                    }
                    finally
                    {
                        if (fileSocket != null)
                        {
                            sktLst.TryRemove(((IPEndPoint)fileSocket.LocalEndPoint).toLong(), out var tp);
                            fileSocket.safeClose();
                        }
                    }


                });

                
            }
        }
        public void getFile(string filename,int inusertoken = -1, string localName=null)
        {
            if (available)
            {
                Task.Factory.StartNew(() =>
                {
                    Socket fileSocket = null;
                    try
                    {
                        fileSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        fileSocket.Connect(srvhost);
                        if (fileSocket.isAlive())
                        {
                            sktLst.TryAdd(((IPEndPoint)fileSocket.LocalEndPoint).toLong(), fileSocket);
                            fileSocket.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                            var buffer = (new comInfo { cType = comType.main, mainIP = lcEDP.toLong() }).toBin();
                            fileSocket.sendAllPack(buffer);
                            var tpIfo = new comInfo { fileName = filename, fType = fileType.receive, cType = comType.file, usertoken = inusertoken };
                            fileSocket.sendAllPack(tpIfo.toBin());
                            var lenbf = new byte[8];
                            if (fileSocket.receiveAll(ref lenbf))
                            {
                                var len = BitConverter.ToInt64(lenbf, 0);
                                if (len < 0)
                                {
                                    if (recData!=null)
                                    {
                                        Task.Factory.StartNew(() => recData.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken)));
                                    }
                                }
                                else
                                {
                                    if (string.IsNullOrWhiteSpace(localName))
                                    {
                                        localName = tempDirectory + Path.GetFileName(filename);
                                        if (File.Exists(localName))
                                        {
                                            localName = tempDirectory + Path.GetFileNameWithoutExtension(filename) + "_" + DateTime.Now.ToString("yyyyMMddHHmmss") + Path.GetExtension(filename);
                                        }
                                    }
                                    var realfile = localName;
                                    var tmpfile = realfile + ".tmp";
                                    long cd = 0;
                                    if (tcpTools.genTempFile(tmpfile, len))
                                    {
                                        using (var mf = MemoryMappedFile.CreateFromFile(tmpfile, FileMode.Open))
                                        {
                                            using (var mvs = mf.CreateViewAccessor())
                                            {
                                                byte[] filebuff = new byte[txBuff];
                                                Stopwatch stp = new Stopwatch();
                                                long spd = 0;
                                                long rest = 0;
                                                stp.Start();
                                                while (cd < len)
                                                {
                                                    if (!fileSocket.isAlive())
                                                    {
                                                        break;
                                                    }
                                                    rest = len - cd;

                                                    if (rest <= txBuff)
                                                    {
                                                        if (!fileSocket.receiveAll(ref filebuff, (int)rest))
                                                        {
                                                            break;
                                                        }
                                                        mvs.WriteArray(cd, filebuff, 0, (int)rest);
                                                        cd += rest;
                                                    }
                                                    else
                                                    {
                                                        if (!fileSocket.receiveAll(ref filebuff))
                                                        {
                                                            break;
                                                        }
                                                        mvs.WriteArray(cd, filebuff, 0, txBuff);
                                                        cd += txBuff;
                                                    }
                                                    if (recFileRate != null)
                                                    {
                                                        var dt = cd;
                                                        Task.Factory.StartNew(() =>
                                                        {
                                                            var rate = dt / (double)len;
                                                            recFileRate(this, new fileArg { endp = srvhost, fileName = realfile, fileState = fileType.receive, str = rate.ToString("p2"), value = rate, userToken = inusertoken });
                                                        });
                                                    }

                                                    if (recFileSpeed != null)
                                                    {
                                                        if (spd == 0 || (spd > 0 && stp.Elapsed.TotalSeconds >= 1))
                                                        {
                                                            var dt = cd - spd;
                                                            spd = cd;
                                                            var sj = stp.Elapsed.TotalSeconds;
                                                            Task.Factory.StartNew(() =>
                                                            {
                                                                var speed = dt / sj;
                                                                recFileSpeed(this, new fileArg { endp = srvhost, fileName = realfile, fileState = fileType.receive, str = speed.fileSize() + "/s", value = speed, userToken = inusertoken });
                                                            });
                                                            stp.Restart();
                                                        }

                                                    }
                                                }
                                                stp.Stop();

                                            }
                                        }
                                        Task.Factory.StartNew(() =>
                                        {
                                            if (cd != len)
                                            {
                                                Thread.Sleep(500);
                                                File.Delete(tmpfile);
                                                recData?.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken));
                                            }
                                            else
                                            {
                                                File.Move(tmpfile, realfile);
                                                recData?.Invoke(srvhost, new netArg( true, true, realfile.toBin(), inusertoken));
                                            }
                                        });
                                    }
                                    else
                                    {
                                        if (recData != null)
                                        {
                                            Task.Factory.StartNew(() => recData.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken)));
                                        }
                                    }



                                }
                            }
                            else
                            {
                                if (recData != null)
                                {
                                    Task.Factory.StartNew(() => recData.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken)));
                                }
                            }
                        }
                        else
                        {
                            if (recData != null)
                            {
                                Task.Factory.StartNew(() => recData.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken)));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (recData != null)
                        {
                            Task.Factory.StartNew(() => recData.Invoke(srvhost, new netArg( true, false, filename.toBin(), inusertoken)));
                        }
                        if (!ownClose)
                        {
                            callEvent(errorRec, new infoArg { iType = infoType.cli_getfile, message = ex.Message });
                        }

                    }
                    finally
                    {
                        if (fileSocket != null)
                        {
                            sktLst.TryRemove(((IPEndPoint)fileSocket.LocalEndPoint).toLong(), out var tp);
                            fileSocket.safeClose();
                        }
                    }
                });
            }
            


        }
        private void send(comInfo tmpData,bool canwait=true)
        {

            try
            {
                if (!workSocket.isAlive())
                {
                    close();
                    if (!ownClose)
                    {
                        callEvent(disConnected, new infoArg { endp = srvhost, iType = infoType.cli_sendStart });
                    }
                }
                else if (isSending&&canwait)
                {
                    cachesend.Enqueue(tmpData);
                }
                else
                {
                    if (canwait)
                    {
                        isSending = true;
                    }

                    sdarg.UserToken = tmpData.usertoken;
                    var buffer = tmpData.toBin();
                    var tpack = new SendPacketsElement[2];
                    tpack[0] = new SendPacketsElement(BitConverter.GetBytes(buffer.LongLength));
                    tpack[1] = new SendPacketsElement(buffer);
                    sdarg.SendPacketsElements = tpack;
                    workSocket.SendPacketsAsync(sdarg);

                }

            }
            catch (Exception ex)
            {
#if debug
                    throw;
#endif
                close();

                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_sendObj, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_sendObj, message = ex.Message });
                }
            }
        }

        private void Sdarg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                if (e.SocketError == SocketError.Success)
                {
                    callEvent(sendOver, new infoArg { endp = srvhost, isok = true, userToken = (int)e.UserToken, iType = infoType.cli_sendComplete }, true);
                }
                else
                {
                    callEvent(sendOver, new infoArg { endp = srvhost, isok = false, userToken = (int)e.UserToken, iType = infoType.cli_sendComplete }, true);
                }
                if (cachesend.Count > 0)
                {
                    if (cachesend.TryDequeue(out var tmp))
                    {
                        send(tmp,false);
                    }
                    else
                    {
                        isSending = false;
                    }
                }
                else
                {
                    isSending = false;

                }
            }
            catch (Exception ex)
            {
#if debug
                    throw;
#endif
                close();


                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_sendComplete, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_sendComplete, message = ex.Message });
                }
            }
        }
        public T getReply<T>(object inObj, int inusertoken = -1, int timeout = 60000)
        {
            try
            {
                if (available)
                {
                    if (timeout <= 0)
                    {
                        timeout = 1;
                    }
                    var objid = Guid.NewGuid();
                    var tifo = new replyIfo();
                    if (replyCache.TryAdd(objid, tifo))
                    {
                        var fsdata = new comInfo { cType=comType.obj, bin = inObj.toBin(), id = objid, usertoken = inusertoken };
                        send(fsdata);
                        if (tifo.autoEvent.WaitOne(timeout))
                        {
                            if (tifo.bin!=null)
                            {
                                var ret = tifo.bin.fromBin<T>();
                                replyCache.TryRemove(objid, out var tmp);
                                return ret;
                            }
                            else
                            {
                                replyCache.TryRemove(objid, out var tmp);
                                return default(T);
                            }

                        }
                        else
                        {
                            replyCache.TryRemove(objid, out var tmp);
                            return default(T);
                        }

                    }
                    else
                    {
                        var fsdata = new comInfo { cType=comType.obj, bin = inObj.toBin(), id = Guid.Empty, usertoken = inusertoken };
                        send(fsdata);
                        return default(T);
                    }
                }
                else
                {
                    return default(T);
                }
 

            }
            catch(Exception ex)
            {
#if debug
                throw;
#endif
                close();
                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_getReply, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_getReply, message = ex.Message });

                }
                return default(T);
            }
        }
        public void sendObject(object inObj, int inusertoken = -1)
        {
            try
            {
                if (available)
                {
                    var fsdata = new comInfo { cType = comType.obj, bin = inObj.toBin(), id = Guid.Empty, usertoken = inusertoken };
                    send(fsdata);
                }

            }
            catch(Exception ex)
            {
#if debug
                throw;
#endif
                close();
                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_sendPlan, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_sendPlan, message = ex.Message });

                }
            }
        }
        private IPEndPoint getP2pSrv(IPEndPoint deIP)
        {
            try
            {
                if (available)
                {
                    int timeout = 60000;
                    var objid = Guid.NewGuid();
                    var tifo = new replyIfo();
                    if (replyCache.TryAdd(objid, tifo))
                    {
                        var fsdata = new comInfo { cType = comType.p2p, mainIP = deIP.toLong(), id = objid };
                        send(fsdata);
                        if (tifo.autoEvent.WaitOne(timeout))
                        {
                            if (tifo.bin==null)
                            {
                                replyCache.TryRemove(objid, out var tmp);
                                return null;
                            }
                            else
                            {
                                var ret = tifo.bin.fromBin<long>().toIP();
                                replyCache.TryRemove(objid, out var tmp);
                                return ret;
                            }

                        }
                        else
                        {
                            replyCache.TryRemove(objid, out var tmp);
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
            catch (Exception ex)
            {

#if debug
                throw;
#endif
                close();
                if (!ownClose)
                {
                    callEvent(disConnected, new infoArg { iType = infoType.cli_getP2pSrv, message = ex.Message });
                    callEvent(errorRec, new infoArg { iType = infoType.cli_getP2pSrv, message = ex.Message });

                }
                return null;
            }
        }
        public void sendObjectP2P(IPEndPoint deIP,object inObj,int inusertoken = -1)
        {
            if (available)
            {
                Task.Factory.StartNew(() =>
                {
                    var srvip = getP2pSrv(deIP);
                    Socket p2pclient = null;
                    try
                    {
                        if (srvip != null)
                        {
                            p2pclient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            p2pclient.Connect(srvip);
                            if (p2pclient.isAlive())
                            {
                                sktLst.TryAdd(((IPEndPoint)p2pclient.RemoteEndPoint).toLong(), p2pclient);
                                p2pclient.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                                var fsdata = new comInfo { cType = comType.obj, mainIP = lcEDP.toLong(), bin = inObj.toBin(), id = Guid.Empty, usertoken = inusertoken };
                                if (p2pclient.sendAllPack(fsdata.toBin()))
                                {
                                    sendOver?.Invoke(this, new infoArg { endp = deIP, isok = true, iType = infoType.cli_sendObjP2P, userToken = inusertoken });
                                }
                                else
                                {
                                    sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendObjP2P, userToken = inusertoken });
                                }
                            }
                            else
                            {
                                sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendObjP2P, userToken = inusertoken });
                            }
                        }
                        else
                        {
                            sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendObjP2P, userToken = inusertoken });
                        }

                    }
                    catch (Exception ex)
                    {
                        sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendObjP2P, userToken = inusertoken });
                        if (!ownClose)
                        {
                            callEvent(errorRec, new infoArg { iType = infoType.cli_sendObjP2P, message = ex.Message });
                        }
                    }
                    finally
                    {
                        if (p2pclient != null)
                        {
                            sktLst.TryRemove(((IPEndPoint)p2pclient.LocalEndPoint).toLong(), out var tp);
                            p2pclient.safeClose();
                        }
                    }
                });

            }

        }
        public T getReplyP2P<T>(IPEndPoint deIP,object inObj, int inusertoken = -1, int timeout = 60000)
        {
            if (available)
            {
                var srvip = getP2pSrv(deIP);
                Socket p2pclient = null;
                try
                {
                    if (srvip != null)
                    {
                        if (timeout <= 0)
                        {
                            timeout = 1;
                        }
                        p2pclient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        p2pclient.Connect(srvip);
                        if (p2pclient.isAlive())
                        {
                            sktLst.TryAdd(((IPEndPoint)p2pclient.RemoteEndPoint).toLong(), p2pclient);
                            p2pclient.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                            var fsdata = new comInfo { cType = comType.obj, mainIP = lcEDP.toLong(), bin = inObj.toBin(), id = Guid.NewGuid(), usertoken = inusertoken };
                            if (p2pclient.sendAllPack(fsdata.toBin()))
                            {
                                var aret = new AutoResetEvent(false);
                                comInfo jg = null;
                                var p2pArg = new SocketAsyncEventArgs();
                                p2pArg.SetBuffer(new byte[8], 0, 8);
                                p2pArg.Completed += (s, e) =>
                                {
                                    try
                                    {
                                        if (e.SocketError == SocketError.Success && e.BytesTransferred == 8)
                                        {
                                            var length = BitConverter.ToInt64(e.Buffer, 0);
                                            var buffer = new byte[length];
                                            if (p2pclient.receiveAll(ref buffer))
                                            {
                                                jg = buffer.fromBin<comInfo>();
                                            }
                                        }
                                    }
                                    catch (Exception)
                                    {

                                        throw;
                                    }
                                    finally
                                    {
                                        aret.Set();
                                    }
                                };
                                p2pclient.ReceiveAsync(p2pArg);
                                if (aret.WaitOne(timeout))
                                {
                                    if (jg!=null)
                                    {
                                        if (jg.bin!=null)
                                        {
                                            return jg.bin.fromBin<T>();
                                        }
                                        else
                                        {
                                            return default(T);
                                        }
                                        
                                    }
                                    else
                                    {
                                        return default(T);
                                    }
                                }
                                else
                                {
                                    return default(T);
                                }
                            }
                            else
                            {
                                return default(T);
                            }
                        }
                        else
                        {
                            return default(T);
                        }

                    }
                    else
                    {
                        return default(T);
                    }

                }
                catch (Exception ex)
                {


                    if (!ownClose)
                    {
                        callEvent(errorRec, new infoArg { iType = infoType.cli_getReplyP2P, message = ex.Message });
                    }
                    return default(T);
                }
                finally
                {
                    if (p2pclient != null)
                    {
                        sktLst.TryRemove(((IPEndPoint)p2pclient.LocalEndPoint).toLong(), out var tp);
                        p2pclient.safeClose();
                    }
                }
            }
            else
            {
                return default(T);
            }

        }
        public void sendFileP2P(IPEndPoint deIP,string filename, int inusertoken = -1)
        {
            if (available&&File.Exists(filename))
            {
                var srvip = getP2pSrv(deIP);
                Socket p2pclient = null;
                try
                {
                    if (srvip!=null)
                    {
                        p2pclient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        p2pclient.Connect(srvip);
                        if (p2pclient.isAlive())
                        {
                            sktLst.TryAdd(((IPEndPoint)p2pclient.RemoteEndPoint).toLong(), p2pclient);
                            p2pclient.IOControl(IOControlCode.KeepAliveValues, tcpTools.keepAliveValues(1, 1000, 1000), null);
                            var ifo = new FileInfo(filename);
                            var tpIfo = new comInfo { fileName = filename, fileSize = ifo.Length, mainIP=lcEDP.toLong(),fType = fileType.send, cType = comType.file, usertoken = inusertoken };
                            p2pclient.sendAllPack(tpIfo.toBin());
                            var sdbf = new byte[1];
                            if (p2pclient.receiveAll(ref sdbf))
                            {
                                if (sdbf[0] == 1)
                                {
                                    using (var mf = MemoryMappedFile.CreateFromFile(filename, FileMode.Open))
                                    {
                                        using (var mvs = mf.CreateViewAccessor())
                                        {
                                            long cd = 0;
                                            long rest = 0;
                                            Stopwatch stp = new Stopwatch();
                                            long spd = 0;
                                            byte[] filebuff = new byte[txBuff];
                                            stp.Start();
                                            while (cd < ifo.Length)
                                            {
                                                if (!p2pclient.isAlive())
                                                {
                                                    break;
                                                }
                                                rest = ifo.Length - cd;
                                                if (rest <= txBuff)
                                                {

                                                    mvs.ReadArray(cd, filebuff, 0, (int)rest);
                                                    if (!p2pclient.sendAll(filebuff, (int)rest))
                                                    {
                                                        break;
                                                    }
                                                    cd += rest;
                                                }
                                                else
                                                {
                                                    mvs.ReadArray(cd, filebuff, 0, txBuff);
                                                    if (!p2pclient.sendAll(filebuff))
                                                    {
                                                        break;
                                                    }
                                                    cd += txBuff;
                                                }
                                                if (sendFileRate != null)
                                                {
                                                    var dt = cd;
                                                    Task.Factory.StartNew(() =>
                                                    {
                                                        var rate = dt / (double)ifo.Length;
                                                        sendFileRate(this, new fileArg { endp = deIP, fileName = filename, fileState = fileType.send, str = rate.ToString("p2"), value = rate, userToken = inusertoken });
                                                    });
                                                }
                                                if (sendFileSpeed != null)
                                                {
                                                    if (spd == 0 || (spd > 0 && stp.Elapsed.TotalSeconds >= 1))
                                                    {
                                                        var dt = cd - spd;
                                                        spd = cd;
                                                        var sj = stp.Elapsed.TotalSeconds;
                                                        Task.Factory.StartNew(() =>
                                                        {
                                                            var speed = dt / sj;
                                                            sendFileSpeed(this, new fileArg { endp = deIP, fileName = filename, fileState = fileType.send, str = speed.fileSize() + "/s", value = speed, userToken = inusertoken });
                                                        });
                                                        stp.Restart();
                                                    }
                                                }
                                                if (fileDelay > 0)
                                                {
                                                    Thread.Sleep(fileDelay);
                                                }

                                            }
                                            stp.Stop();
                                            if (cd != ifo.Length)
                                            {
                                                sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message=filename,userToken = inusertoken });
                                            }
                                            else
                                            {
                                                sendOver?.Invoke(this, new infoArg { endp = deIP, isok = true, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                                            }
                                        }
                                    }

                                }
                                else
                                {
                                    sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                                }
                            }
                            else
                            {
                                sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                            }


                        }
                        else
                        {
                            sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                        }
                    }
                    else
                    {
                        sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                    }

                }
                catch (Exception ex)
                {
                    sendOver?.Invoke(this, new infoArg { endp = deIP, isok = false, iType = infoType.cli_sendfileP2P, message = filename, userToken = inusertoken });
                    if (!ownClose)
                    {
                        callEvent(errorRec, new infoArg { iType = infoType.cli_sendfileP2P, message = ex.Message });
                    }

                }
                finally
                {
                    if (p2pclient != null)
                    {
                        sktLst.TryRemove(((IPEndPoint)p2pclient.LocalEndPoint).toLong(), out var tp);
                        p2pclient.safeClose();
                    }
                }
            }
        }
        public void disConnect()
        {
            close(true);
        }
        private void close(bool inownclose=false)
        {
            try
            {
                if (inownclose)
                {
                    ownClose = inownclose;
                }

                foreach (var item in sktLst.Values)
                {
                    if (item!=null)
                    {
                        item.safeClose();
                    }
                }
                if (p2pSocket != null)
                {
                    p2pSocket.safeClose();
                }
                if (workSocket != null)
                {
                    workSocket.safeClose();
                }
                iplock.EnterWriteLock();
                lcEDP = null;
                iplock.ExitWriteLock();
            }
            catch
            {
#if debug
                throw;
#endif
                ownClose = inownclose;

                foreach (var item in sktLst.Values)
                {
                    if (item != null)
                    {
                        item.safeClose();
                    }
                }
                if (p2pSocket != null)
                {
                    p2pSocket.safeClose();
                }
                if (workSocket != null)
                {
                    workSocket.safeClose();
                }

                iplock.EnterWriteLock();
                lcEDP = null;
                iplock.ExitWriteLock();
                
            }
        }

    }

    internal delegate byte[] ipcevent(string sender, byte[] sdbin);
    public class ipcArg
    {
        private byte[] bin { get; set; }
        public ipcArg(byte[] inbin)
        {
            bin = inbin;
        }
        public T getData<T>()
        {
            if (bin == null)
            {
                return default(T);
            }
            else
            {
                return bin.fromBin<T>();
            }

        }
    }
    public delegate object ipcMess(string sender, ipcArg e);
    internal class dataobj : MarshalByRefObject
    {
        public event ipcevent OnDataRec;
        public byte[] sendBin(string src, byte[] inbin)
        {
            return OnDataRec?.Invoke(src, inbin);
        }


    }
    public class ipcClient
    {
        private IpcServerChannel server;
        //private IpcClientChannel client;
        private dataobj midobj;
        private dataobj rmtobj;
        public event ipcMess OnRecieve;
        private string localName;
        public bool isRun { get; private set; } = false;
        public ipcClient(string srvName)
        {
            BinaryServerFormatterSinkProvider serverProvider = new BinaryServerFormatterSinkProvider();
            serverProvider.TypeFilterLevel = System.Runtime.Serialization.Formatters.TypeFilterLevel.Full;
            IDictionary props = new Hashtable();
            props["name"] = srvName;
            props["portName"] = srvName;
            props["typeFilterLevel"] = TypeFilterLevel.Full;
            localName = srvName;
            server = new IpcServerChannel(props, serverProvider, null);
            //client = new IpcClientChannel(props, null);
        }
        ~ipcClient()
        {
            stop();
        }
        public bool start()
        {
            if (!isRun)
            {
                try
                {
                    ChannelServices.RegisterChannel(server, false);
                    midobj = new dataobj();
                    RemotingServices.Marshal(midobj, "srv");
                    midobj.OnDataRec += Midobj_OnDataRec;
                    isRun = true;
                    return true;
                }
                catch (Exception)
                {
                    isRun = false;
                    return false;
                }
            }
            else
            {
                return false;
            }

        }
        public void stop()
        {
            if (isRun)
            {
                RemotingServices.Disconnect(midobj);
                ChannelServices.UnregisterChannel(server);
                isRun = false;
            }
        }
        private byte[] Midobj_OnDataRec(string sender, byte[] e)
        {
            return OnRecieve?.Invoke(sender, new ipcArg(e)).toBin();
        }
        public void setRmtSrv(string rmtName)
        {
            rmtobj = (dataobj)Activator.GetObject(typeof(dataobj), $"ipc://{rmtName}/srv");
        }
        public void sendObject(object inObj)
        {
            try
            {
                if (rmtobj != null)
                {
                    rmtobj.sendBin(localName, inObj.toBin());
                }
            }
            catch
            {

                return;
            }

        }
        public void sendObject(string rmtName, object inObj)
        {
            try
            {
                var srvobj = (dataobj)Activator.GetObject(typeof(dataobj), $"ipc://{rmtName}/srv");
                srvobj.sendBin(localName, inObj.toBin());
            }
            catch
            {
                return;
            }

        }
        public T getReply<T>(object inObj)
        {
            try
            {
                if (rmtobj != null)
                {
                    return rmtobj.sendBin(localName, inObj.toBin()).fromBin<T>();
                }
                else
                {
                    return default(T);
                }
            }
            catch
            {

                return default(T);
            }

        }
        public T getReply<T>(string rmtName, object inObj)
        {
            try
            {
                var srvobj = (dataobj)Activator.GetObject(typeof(dataobj), $"ipc://{rmtName}/srv");
                return srvobj.sendBin(localName, inObj.toBin()).fromBin<T>();
            }
            catch
            {
                return default(T);
            }

        }

    }
    public static class ex
    {
        public static byte[] toBin(this object inobj)
        {
            return HproseFormatter.Serialize(inobj).ToArray();

        }
        public static T fromBin<T>(this byte[] inbin)
        {
            return HproseFormatter.Unserialize<T>(inbin);
        }
        public static byte[] DEFcompress(this byte[] inbin)
        {
            var ms = new MemoryStream();
            var zip = new DeflateStream(ms, CompressionMode.Compress);
            zip.Write(inbin, 0, inbin.Length);
            zip.Close();
            return ms.ToArray();
        }
        public static byte[] DEFdecompress(this byte[] inbin)
        {
            var ms = new MemoryStream();
            var zip = new DeflateStream(new MemoryStream(inbin), CompressionMode.Decompress);
            zip.CopyTo(ms);
            zip.Close();
            return ms.ToArray();
        }
        public static byte[] GZIPcompress(this byte[] inbin)
        {
            var ms = new MemoryStream();
            var zip = new GZipStream(ms, CompressionMode.Compress);
            zip.Write(inbin, 0, inbin.Length);
            zip.Close();
            return ms.ToArray();
        }
        public static byte[] GZIPdecompress(this byte[] inbin)
        {
            var ms = new MemoryStream();
            var zip = new GZipStream(new MemoryStream(inbin), CompressionMode.Decompress);
            zip.CopyTo(ms);
            zip.Close();
            return ms.ToArray();
        }

    }

    #region 序列化
    internal class CtorAccessor
    {
        private delegate object NewInstanceDelegate();
        private static readonly Type typeofInt8 = typeof(sbyte);
        private static readonly Type typeofUInt8 = typeof(byte);
        private static readonly Type typeofBoolean = typeof(bool);
        private static readonly Type typeofInt16 = typeof(short);
        private static readonly Type typeofUInt16 = typeof(ushort);
        private static readonly Type typeofChar = typeof(char);
        private static readonly Type typeofInt32 = typeof(int);
        private static readonly Type typeofUInt32 = typeof(uint);
        private static readonly Type typeofInt64 = typeof(long);
        private static readonly Type typeofUInt64 = typeof(ulong);
        private static readonly Type typeofSingle = typeof(float);
        private static readonly Type typeofDouble = typeof(double);
        private static readonly Type typeofObject = typeof(object);
        private static readonly Type[] zeroTypeArray = new Type[0];
        private static readonly ParameterModifier[] zeroParameterModifierArray = new ParameterModifier[0];
        private static readonly Type typeofNewInstanceDelegate = typeof(NewInstanceDelegate);
        NewInstanceDelegate newInstanceDelegate;
        private static readonly ReaderWriterLockSlim ctorAccessorsCacheLock = new ReaderWriterLockSlim();
        private static readonly Dictionary<Type, CtorAccessor> ctorAccessorsCache = new Dictionary<Type, CtorAccessor>();

        private class ConstructorComparator : IComparer<ConstructorInfo>
        {
            public int Compare(ConstructorInfo x, ConstructorInfo y)
            {
                return x.GetParameters().Length - y.GetParameters().Length;
            }
        }
        private CtorAccessor(Type type)
        {
            if (type.IsValueType)
            {
                DynamicMethod dynamicNewInstance = new DynamicMethod("$NewInstance",
                    typeofObject,
                    zeroTypeArray,
                    type,
                    true);
                ILGenerator newInstanceGen = dynamicNewInstance.GetILGenerator();
                LocalBuilder v = newInstanceGen.DeclareLocal(type);
                newInstanceGen.Emit(OpCodes.Ldloca_S, v);
                newInstanceGen.Emit(OpCodes.Initobj, type);
                newInstanceGen.Emit(OpCodes.Ldloc_S, v);
                newInstanceGen.Emit(OpCodes.Box, type);
                newInstanceGen.Emit(OpCodes.Ret);
                newInstanceDelegate = (NewInstanceDelegate)dynamicNewInstance.CreateDelegate(typeofNewInstanceDelegate);
                return;
            }
            BindingFlags bindingflags = BindingFlags.Instance |
                            BindingFlags.Public |
                            BindingFlags.NonPublic |
                            BindingFlags.FlattenHierarchy;
            ConstructorInfo ctor = type.GetConstructor(bindingflags, null, zeroTypeArray, zeroParameterModifierArray);
            if (ctor != null)
            {
                DynamicMethod dynamicNewInstance = new DynamicMethod("$NewInstance",
                    typeofObject,
                    zeroTypeArray,
                    type,
                    true);
                ILGenerator newInstanceGen = dynamicNewInstance.GetILGenerator();
                newInstanceGen.Emit(OpCodes.Newobj, ctor);
                newInstanceGen.Emit(OpCodes.Ret);
                newInstanceDelegate = (NewInstanceDelegate)dynamicNewInstance.CreateDelegate(typeofNewInstanceDelegate);
            }
            else
            {
                ConstructorInfo[] ctors = type.GetConstructors(bindingflags);
                Array.Sort(ctors, 0, ctors.Length, new ConstructorComparator());
                for (int i = 0, length = ctors.Length; i < length; ++i)
                {
                    try
                    {
                        DynamicMethod dynamicNewInstance = new DynamicMethod("$NewInstance",
                            typeofObject,
                            zeroTypeArray,
                            type,
                            true);
                        ParameterInfo[] pi = ctors[i].GetParameters();
                        int piLength = pi.Length;
                        ILGenerator newInstanceGen = dynamicNewInstance.GetILGenerator();
                        for (int j = 0; j < piLength; j++)
                        {
                            Type parameterType = pi[j].ParameterType;
                            if (parameterType == typeofInt8 ||
                                parameterType == typeofBoolean ||
                                parameterType == typeofUInt8 ||
                                parameterType == typeofInt16 ||
                                parameterType == typeofUInt16 ||
                                parameterType == typeofChar ||
                                parameterType == typeofInt32 ||
                                parameterType == typeofUInt32)
                            {
                                newInstanceGen.Emit(OpCodes.Ldc_I4_0);
                            }
                            else if (parameterType == typeofInt64 ||
                                     parameterType == typeofUInt64)
                            {
                                newInstanceGen.Emit(OpCodes.Ldc_I8, (long)0);
                            }
                            else if (parameterType == typeofSingle)
                            {
                                newInstanceGen.Emit(OpCodes.Ldc_R4, (float)0);
                            }
                            else if (parameterType == typeofDouble)
                            {
                                newInstanceGen.Emit(OpCodes.Ldc_R8, (double)0);
                            }
                            else if (parameterType.IsValueType)
                            {
                                LocalBuilder v = newInstanceGen.DeclareLocal(parameterType);
                                newInstanceGen.Emit(OpCodes.Ldloca_S, v);
                                newInstanceGen.Emit(OpCodes.Initobj, parameterType);
                                newInstanceGen.Emit(OpCodes.Ldloc_S, v);
                            }
                            else
                            {
                                newInstanceGen.Emit(OpCodes.Ldnull);
                            }
                        }
                        newInstanceGen.Emit(OpCodes.Newobj, ctors[i]);
                        if (type.IsValueType)
                        {
                            newInstanceGen.Emit(OpCodes.Box, type);
                        }
                        newInstanceGen.Emit(OpCodes.Ret);
                        newInstanceDelegate = (NewInstanceDelegate)dynamicNewInstance.CreateDelegate(typeofNewInstanceDelegate);
                        newInstanceDelegate();
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        newInstanceDelegate = null;
                    }
                }
            }
            if (newInstanceDelegate == null)
                throw new NotSupportedException();
        }
        public static CtorAccessor Get(Type type)
        {
            CtorAccessor ctorAccessor = null;
            try
            {
                ctorAccessorsCacheLock.EnterReadLock();

                if (ctorAccessorsCache.TryGetValue(type, out ctorAccessor))
                {
                    return ctorAccessor;
                }
            }
            finally
            {
                ctorAccessorsCacheLock.ExitReadLock();

            }
            try
            {
                ctorAccessorsCacheLock.EnterWriteLock();

                if (ctorAccessorsCache.TryGetValue(type, out ctorAccessor))
                {
                    return ctorAccessor;
                }
                ctorAccessor = new CtorAccessor(type);
                ctorAccessorsCache[type] = ctorAccessor;
            }
            finally
            {
                ctorAccessorsCacheLock.ExitWriteLock();

            }
            return ctorAccessor;
        }
        public object NewInstance()
        {
            return newInstanceDelegate();
        }
    }
    internal class HashMap<TKey, TValue> : IDictionary<TKey, TValue>, IDictionary,
    ICollection<KeyValuePair<TKey, TValue>>, ICollection,
    IEnumerable<KeyValuePair<TKey, TValue>>, IEnumerable
    {
        private Dictionary<TKey, TValue> dict;
        private TValue valueOfNullKey = default(TValue);
        private bool hasNullKey = false;
        public HashMap()
        {
            dict = new Dictionary<TKey, TValue>();
        }
        public HashMap(IDictionary<TKey, TValue> dictionary)
        {
            dict = new Dictionary<TKey, TValue>(dictionary);
        }
        public HashMap(IEqualityComparer<TKey> comparer)
        {
            dict = new Dictionary<TKey, TValue>(comparer);
        }
        public HashMap(int capacity)
        {
            dict = new Dictionary<TKey, TValue>(capacity);
        }
        public HashMap(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
        {
            dict = new Dictionary<TKey, TValue>(dictionary, comparer);
        }
        public HashMap(int capacity, IEqualityComparer<TKey> comparer)
        {
            dict = new Dictionary<TKey, TValue>(capacity, comparer);
        }
        public IEqualityComparer<TKey> Comparer
        {
            get
            {
                return dict.Comparer;
            }
        }
        public int Count
        {
            get
            {
                return dict.Count + (hasNullKey ? 1 : 0);
            }
        }
        public TValue this[TKey key]
        {
            get
            {
                if (key == null)
                {
                    if (hasNullKey) return valueOfNullKey;
                    throw new KeyNotFoundException();
                }
                return dict[key];
            }
            set
            {
                if (key == null)
                {
                    valueOfNullKey = value;
                    hasNullKey = true;
                }
                else
                {
                    dict[key] = value;
                }
            }
        }
        public KeyCollection Keys
        {
            get
            {
                return new KeyCollection(this, dict.Keys);
            }
        }
        public ValueCollection Values
        {
            get
            {
                return new ValueCollection(this, dict.Values);
            }
        }
        public void Add(TKey key, TValue value)
        {
            if (key == null)
            {
                if (hasNullKey) throw new ArgumentException("An element with the same key already exists in the dictionary.");
                valueOfNullKey = value;
                hasNullKey = true;
            }
            else
            {
                dict.Add(key, value);
            }
        }
        public void Clear()
        {
            valueOfNullKey = default(TValue);
            hasNullKey = false;
            dict.Clear();
        }
        public bool ContainsKey(TKey key)
        {
            if (key == null) return hasNullKey;
            return dict.ContainsKey(key);
        }
        public bool ContainsValue(TValue value)
        {
            if (hasNullKey)
            {
                IEqualityComparer<TValue> cmp = EqualityComparer<TValue>.Default;
                if (cmp.Equals(valueOfNullKey, value)) return true;
            }
            return dict.ContainsValue(value);
        }
        public Enumerator GetEnumerator()
        {
            return new Enumerator(this, dict.GetEnumerator());
        }
        public bool Remove(TKey key)
        {
            if (key == null)
            {
                if (hasNullKey)
                {
                    valueOfNullKey = default(TValue);
                    hasNullKey = false;
                    return true;
                }
                return false;
            }
            else
            {
                return dict.Remove(key);
            }
        }
        public bool TryGetValue(TKey key, out TValue value)
        {
            if (key == null)
            {
                value = hasNullKey ? valueOfNullKey : default(TValue);
                return hasNullKey;
            }
            return dict.TryGetValue(key, out value);
        }
        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> keyValuePair)
        {
            Add(keyValuePair.Key, keyValuePair.Value);
        }
        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> keyValuePair)
        {
            TValue value;
            if (!TryGetValue(keyValuePair.Key, out value)) return false;
            return EqualityComparer<TValue>.Default.Equals(keyValuePair.Value, value);
        }
        private void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            if (hasNullKey)
            {
                ((ICollection<KeyValuePair<TKey, TValue>>)dict).CopyTo(array, index + 1);
                array[index] = new KeyValuePair<TKey, TValue>(default(TKey), valueOfNullKey);
            }
            else
            {
                ((ICollection<KeyValuePair<TKey, TValue>>)dict).CopyTo(array, index);
            }
        }
        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            CopyTo(array, index);
        }
        void ICollection.CopyTo(Array array, int index)
        {
            CopyTo((KeyValuePair<TKey, TValue>[])array, index);
        }
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get
            {
                return false;
            }
        }
        bool ICollection.IsSynchronized
        {
            get
            {
                return false;
            }
        }
        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> keyValuePair)
        {
            if (keyValuePair.Key == null)
            {
                if (hasNullKey)
                {
                    valueOfNullKey = default(TValue);
                    hasNullKey = false;
                    return true;
                }
                return false;
            }
            return ((ICollection<KeyValuePair<TKey, TValue>>)dict).Remove(keyValuePair);
        }
        object ICollection.SyncRoot
        {
            get
            {
                return ((ICollection)dict).SyncRoot;
            }
        }
        static T ToT<T>(object obj, string paramName)
        {
            if ((obj == null) && (default(T) != null) || !(obj is T))
            {
                throw new ArgumentException("not of type: " + typeof(T).ToString(), paramName);
            }
            return (T)obj;
        }
        void IDictionary.Add(object key, object value)
        {
            this.Add(ToT<TKey>(key, "key"), ToT<TValue>(value, "value"));
        }
        bool IDictionary.Contains(object key)
        {
            if (key == null) return hasNullKey;
            return ((IDictionary)dict).Contains(key);
        }
        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return new HashMapEnumerator(this, ((IDictionary)dict).GetEnumerator());
        }
        bool IDictionary.IsFixedSize
        {
            get
            {
                return false;
            }
        }
        bool IDictionary.IsReadOnly
        {
            get
            {
                return false;
            }
        }
        object IDictionary.this[object key]
        {
            get
            {
                if (key == null)
                {
                    if (hasNullKey) return valueOfNullKey;
                }
                else
                {
                    TKey k = ToT<TKey>(key, "key");
                    if (ContainsKey(k)) return this[k];
                }
                return null;
            }
            set
            {
                this[ToT<TKey>(key, "key")] = ToT<TValue>(value, "value");
            }
        }
        ICollection<TKey> IDictionary<TKey, TValue>.Keys
        {
            get { return Keys; }
        }
        ICollection IDictionary.Keys
        {
            get { return Keys; }
        }
        void IDictionary.Remove(object key)
        {
            if (key == null)
            {
                valueOfNullKey = default(TValue);
                hasNullKey = false;
            }
            else
            {
                ((IDictionary)dict).Remove(key);
            }
        }
        ICollection<TValue> IDictionary<TKey, TValue>.Values
        {
            get { return Values; }
        }
        ICollection IDictionary.Values
        {
            get { return Values; }
        }
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            return new Enumerator(this, dict.GetEnumerator());
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this, dict.GetEnumerator());
        }

        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>,
            IDisposable, IDictionaryEnumerator, IEnumerator
        {
            private Dictionary<TKey, TValue>.Enumerator e;
            private HashMap<TKey, TValue> m;
            private int p;

            internal Enumerator(HashMap<TKey, TValue> m, Dictionary<TKey, TValue>.Enumerator e)
            {
                this.m = m;
                this.e = e;
                this.p = -1;
            }
            public bool MoveNext()
            {
                if (++p > 0) return e.MoveNext();
                if (m.hasNullKey) return true;
                return e.MoveNext();
            }
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if ((p == 0) && m.hasNullKey)
                    {
                        return new KeyValuePair<TKey, TValue>(default(TKey), m.valueOfNullKey);
                    }
                    return e.Current;
                }
            }
            public void Dispose()
            {
            }
            object IEnumerator.Current
            {
                get
                {
                    if ((p == 0) && m.hasNullKey)
                    {
                        return new KeyValuePair<TKey, TValue>(default(TKey), m.valueOfNullKey);
                    }
                    return ((IEnumerator)e).Current;
                }
            }
            void IEnumerator.Reset()
            {
                p = -1;
                ((IEnumerator)e).Reset();
            }
            DictionaryEntry IDictionaryEnumerator.Entry
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return new DictionaryEntry(null, m.valueOfNullKey);
                    }
                    return ((IDictionaryEnumerator)e).Entry;
                }
            }
            object IDictionaryEnumerator.Key
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return null;
                    }
                    return ((IDictionaryEnumerator)e).Key;
                }
            }
            object IDictionaryEnumerator.Value
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return m.valueOfNullKey;
                    }
                    return ((IDictionaryEnumerator)e).Value;
                }
            }
        }

        public struct HashMapEnumerator : IDictionaryEnumerator, IEnumerator
        {
            private IDictionaryEnumerator e;
            private HashMap<TKey, TValue> m;
            private int p;

            internal HashMapEnumerator(HashMap<TKey, TValue> m, IDictionaryEnumerator e)
            {
                this.m = m;
                this.e = e;
                this.p = -1;
            }
            bool IEnumerator.MoveNext()
            {
                if (++p > 0) return ((IEnumerator)e).MoveNext();
                if (m.hasNullKey) return true;
                return ((IEnumerator)e).MoveNext();
            }
            object IEnumerator.Current
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return new DictionaryEntry(null, m.valueOfNullKey);
                    }
                    return e.Current;
                }
            }
            void IEnumerator.Reset()
            {
                p = -1;
                ((IEnumerator)e).Reset();
            }
            DictionaryEntry IDictionaryEnumerator.Entry
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return new DictionaryEntry(null, m.valueOfNullKey);
                    }
                    return e.Entry;
                }
            }
            object IDictionaryEnumerator.Key
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return null;
                    }
                    return e.Key;
                }
            }
            object IDictionaryEnumerator.Value
            {
                get
                {
                    if (p == 0 && m.hasNullKey)
                    {
                        return m.valueOfNullKey;
                    }
                    return e.Value;
                }
            }
        }

        public sealed class KeyCollection : ICollection<TKey>, IEnumerable<TKey>, ICollection, IEnumerable
        {
            private HashMap<TKey, TValue> m;
            private Dictionary<TKey, TValue>.KeyCollection keys;

            internal KeyCollection(HashMap<TKey, TValue> m, Dictionary<TKey, TValue>.KeyCollection keys)
            {
                this.m = m;
                this.keys = keys;
            }
            public void CopyTo(TKey[] array, int index)
            {
                if (m.hasNullKey)
                {
                    keys.CopyTo(array, index + 1);
                    array[index] = default(TKey);
                }
                else
                {
                    keys.CopyTo(array, index);
                }
            }
            public Enumerator GetEnumerator()
            {
                return new Enumerator(m, keys.GetEnumerator());
            }
            void ICollection<TKey>.Add(TKey item)
            {
                ((ICollection<TKey>)keys).Add(item);
            }
            void ICollection<TKey>.Clear()
            {
                ((ICollection<TKey>)keys).Clear();
            }
            bool ICollection<TKey>.Contains(TKey item)
            {
                if (item == null) return m.hasNullKey;
                return ((ICollection<TKey>)keys).Contains(item);
            }
            bool ICollection<TKey>.Remove(TKey item)
            {
                return ((ICollection<TKey>)keys).Remove(item);
            }
            IEnumerator<TKey> IEnumerable<TKey>.GetEnumerator()
            {
                return new Enumerator(m, keys.GetEnumerator());
            }
            void ICollection.CopyTo(Array array, int index)
            {
                if (m.hasNullKey)
                {
                    ((ICollection)keys).CopyTo(array, index + 1);
                    array.SetValue(null, index);
                }
                else
                {
                    ((ICollection)keys).CopyTo(array, index);
                }
            }
            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(m, keys.GetEnumerator());
            }
            public int Count
            {
                get
                {
                    return keys.Count + (m.hasNullKey ? 1 : 0);
                }
            }
            bool ICollection<TKey>.IsReadOnly
            {
                get
                {
                    return true;
                }
            }
            bool ICollection.IsSynchronized
            {
                get
                {
                    return false;
                }
            }
            object ICollection.SyncRoot
            {
                get
                {
                    return ((ICollection)keys).SyncRoot;
                }
            }
            public struct Enumerator : IEnumerator<TKey>, IDisposable, IEnumerator
            {
                private HashMap<TKey, TValue> m;
                private Dictionary<TKey, TValue>.KeyCollection.Enumerator e;
                private int p;
                internal Enumerator(HashMap<TKey, TValue> m, Dictionary<TKey, TValue>.KeyCollection.Enumerator e)
                {
                    this.m = m;
                    this.e = e;
                    this.p = -1;
                }
                public void Dispose()
                {
                }
                public bool MoveNext()
                {
                    if (++p > 0) return e.MoveNext();
                    if (m.hasNullKey) return true;
                    return e.MoveNext();
                }
                public TKey Current
                {
                    get
                    {
                        if ((p == 0) && m.hasNullKey)
                        {
                            return default(TKey);
                        }
                        return e.Current;
                    }
                }
                object IEnumerator.Current
                {
                    get
                    {
                        if ((p == 0) && m.hasNullKey)
                        {
                            return null;
                        }
                        return ((IEnumerator)e).Current;
                    }
                }
                void IEnumerator.Reset()
                {
                    p = -1;
                    ((IEnumerator)e).Reset();
                }
            }
        }
        public sealed class ValueCollection : ICollection<TValue>, IEnumerable<TValue>, ICollection, IEnumerable
        {
            private HashMap<TKey, TValue> m;
            private Dictionary<TKey, TValue>.ValueCollection values;

            internal ValueCollection(HashMap<TKey, TValue> m, Dictionary<TKey, TValue>.ValueCollection values)
            {
                this.m = m;
                this.values = values;
            }
            public void CopyTo(TValue[] array, int index)
            {
                if (m.hasNullKey)
                {
                    values.CopyTo(array, index + 1);
                    array[index] = m.valueOfNullKey;
                }
                else
                {
                    values.CopyTo(array, index);
                }
            }
            public Enumerator GetEnumerator()
            {
                return new Enumerator(m, values.GetEnumerator());
            }
            void ICollection<TValue>.Add(TValue item)
            {
                ((ICollection<TValue>)values).Add(item);
            }
            void ICollection<TValue>.Clear()
            {
                ((ICollection<TValue>)values).Clear();
            }
            bool ICollection<TValue>.Contains(TValue item)
            {
                if (m.hasNullKey)
                {
                    IEqualityComparer<TValue> cmp = EqualityComparer<TValue>.Default;
                    if (cmp.Equals(m.valueOfNullKey, item)) return true;
                }
                return ((ICollection<TValue>)values).Contains(item);
            }
            bool ICollection<TValue>.Remove(TValue item)
            {
                return ((ICollection<TValue>)values).Remove(item);
            }
            IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
            {
                return new Enumerator(m, values.GetEnumerator());
            }
            void ICollection.CopyTo(Array array, int index)
            {
                if (m.hasNullKey)
                {
                    ((ICollection)values).CopyTo(array, index + 1);
                    array.SetValue(m.valueOfNullKey, index);
                }
                else
                {
                    ((ICollection)values).CopyTo(array, index);
                }
            }
            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(m, values.GetEnumerator());
            }
            public int Count
            {
                get
                {
                    return values.Count + (m.hasNullKey ? 1 : 0);
                }
            }
            bool ICollection<TValue>.IsReadOnly
            {
                get
                {
                    return true;
                }
            }
            bool ICollection.IsSynchronized
            {
                get
                {
                    return false;
                }
            }
            object ICollection.SyncRoot
            {
                get
                {
                    return ((ICollection)values).SyncRoot;
                }
            }
            public struct Enumerator : IEnumerator<TValue>, IDisposable, IEnumerator
            {
                private HashMap<TKey, TValue> m;
                private Dictionary<TKey, TValue>.ValueCollection.Enumerator e;
                private int p;
                internal Enumerator(HashMap<TKey, TValue> m, Dictionary<TKey, TValue>.ValueCollection.Enumerator e)
                {
                    this.m = m;
                    this.e = e;
                    this.p = -1;
                }
                public void Dispose()
                {
                }
                public bool MoveNext()
                {
                    if (++p > 0) return e.MoveNext();
                    if (m.hasNullKey) return true;
                    return e.MoveNext();
                }
                public TValue Current
                {
                    get
                    {
                        if ((p == 0) && m.hasNullKey)
                        {
                            return m.valueOfNullKey;
                        }
                        return e.Current;
                    }
                }
                object IEnumerator.Current
                {
                    get
                    {
                        if ((p == 0) && m.hasNullKey)
                        {
                            return m.valueOfNullKey;
                        }
                        return ((IEnumerator)e).Current;
                    }
                }
                void IEnumerator.Reset()
                {
                    p = -1;
                    ((IEnumerator)e).Reset();
                }
            }
        }
    }
    internal class HashMap : Hashtable, IDictionary, ICollection, IEnumerable, ICloneable
    {
        private object valueOfNullKey = null;
        private bool hasNullKey = false;
        public HashMap() : base()
        {
        }
        public HashMap(int capacity) : base(capacity)
        {
        }
        public HashMap(int capacity, float loadFactor) : base(capacity, loadFactor)
        {
        }
        public HashMap(IDictionary value) : base(value)
        {
        }

        public override object Clone()
        {

            HashMap m = (HashMap)base.Clone();
            m.valueOfNullKey = valueOfNullKey;
            m.hasNullKey = hasNullKey;
            return m;
        }
        public override object this[object key]
        {

            get
            {
                if (key == null) return valueOfNullKey;
                return base[key];
            }
            set
            {
                if (key == null)
                {
                    valueOfNullKey = value;
                    hasNullKey = true;
                }
                else
                {
                    base[key] = value;
                }
            }
        }
        public override void Add(object key, object value)
        {

            if (key == null)
            {
                if (hasNullKey) return;
                valueOfNullKey = value;
                hasNullKey = true;
            }
            else
            {
                base.Add(key, value);
            }
        }
        public override bool Contains(object key)
        {

            if (key == null) return hasNullKey;
            return base.Contains(key);
        }
        public override void CopyTo(Array array, int arrayIndex)
        {

            if (hasNullKey)
            {
                base.CopyTo(array, arrayIndex + 1);
                array.SetValue(new DictionaryEntry(null, valueOfNullKey), arrayIndex);

            }
            else
            {
                base.CopyTo(array, arrayIndex);
            }
        }
        public override bool ContainsKey(object key)
        {
            if (key == null) return hasNullKey;
            return base.ContainsKey(key);
        }
        public override bool ContainsValue(object value)
        {
            if (hasNullKey && (valueOfNullKey == value)) return true;
            return base.ContainsValue(value);
        }
        public override void Remove(object key)
        {

            if (key == null)
            {
                valueOfNullKey = null;
                hasNullKey = false;
            }
            else
            {
                base.Remove(key);
            }
        }
        public override int Count
        {

            get
            {
                return base.Count + (hasNullKey ? 1 : 0);
            }
        }
        public override void Clear()
        {

            valueOfNullKey = null;
            hasNullKey = false;
            base.Clear();
        }
        public override IDictionaryEnumerator GetEnumerator()
        {
            IDictionaryEnumerator e = base.GetEnumerator();
            if (hasNullKey)
            {
                return new HashMapEnumerator(e, valueOfNullKey, 3);
            }
            else
            {
                return e;
            }
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            IEnumerator e = base.GetEnumerator();
            if (hasNullKey)
            {
                return new HashMapEnumerator(e, valueOfNullKey, 3);
            }
            else
            {
                return e;
            }
        }
        public override ICollection Keys
        {

            get
            {
                return new KeysCollection(this, base.Keys);
            }
        }
        public override ICollection Values
        {

            get
            {
                return new ValuesCollection(this, base.Values);
            }
        }

        private class HashMapEnumerator :
        IDictionaryEnumerator,
        IEnumerator, ICloneable
        {
            private IEnumerator e;
            private object v;
            private int p;
            private int t;

            internal HashMapEnumerator(IEnumerator e, object v, int t)
            {
                this.e = e;
                this.v = v;
                this.t = t;
                this.p = -1;
            }
            private HashMapEnumerator(IEnumerator e, object v, int t, int p)
            {
                this.e = e;
                this.v = v;
                this.t = t;
                this.p = p;
            }
            public object Clone()
            {
                return new HashMapEnumerator(e, v, t, p);
            }
            public virtual bool MoveNext()
            {
                if (++p > 0)
                {
                    return e.MoveNext();
                }
                return true;
            }
            public virtual void Reset()
            {
                p = -1;
                e.Reset();
            }
            public virtual object Current
            {
                get
                {
                    if (p == 0)
                    {
                        if (t == 1)
                        {
                            return null;
                        }
                        if (t == 2)
                        {
                            return v;
                        }
                        return new DictionaryEntry(null, v);
                    }
                    return e.Current;
                }
            }
            public virtual DictionaryEntry Entry
            {
                get
                {
                    if (p == 0)
                    {
                        return new DictionaryEntry(null, v);
                    }
                    return (DictionaryEntry)e.Current;
                }
            }
            public virtual object Key
            {
                get
                {
                    if (p == 0)
                    {
                        return null;
                    }
                    return ((IDictionaryEnumerator)e).Key;

                }
            }
            public virtual object Value
            {
                get
                {
                    if (p == 0)
                    {
                        return v;
                    }
                    return ((IDictionaryEnumerator)e).Value;

                }
            }
        }

        private class KeysCollection : ICollection, IEnumerable
        {
            private HashMap m;
            private ICollection keys;

            internal KeysCollection(HashMap m, ICollection keys)
            {
                this.m = m;
                this.keys = keys;
            }
            public virtual void CopyTo(Array array, int arrayIndex)
            {
                if (m.hasNullKey)
                {
                    keys.CopyTo(array, arrayIndex + 1);
                    array.SetValue(null, arrayIndex);

                }
                else
                {
                    keys.CopyTo(array, arrayIndex);
                }
            }
            public virtual IEnumerator GetEnumerator()
            {
                IEnumerator e = keys.GetEnumerator();
                if (m.hasNullKey)
                {
                    return new HashMapEnumerator(e, m.valueOfNullKey, 1);
                }
                else
                {
                    return e;
                }
            }
            public virtual int Count
            {
                get
                {
                    return keys.Count + (m.hasNullKey ? 1 : 0);
                }
            }
            public virtual bool IsSynchronized
            {
                get
                {
                    return keys.IsSynchronized;
                }
            }
            public virtual object SyncRoot
            {
                get
                {
                    return keys.SyncRoot;
                }
            }
        }
        private class ValuesCollection : ICollection, IEnumerable
        {
            private HashMap m;
            private ICollection values;

            internal ValuesCollection(HashMap m, ICollection values)
            {
                this.m = m;
                this.values = values;
            }
            public virtual void CopyTo(Array array, int arrayIndex)
            {
                if (m.hasNullKey)
                {
                    values.CopyTo(array, arrayIndex + 1);
                    array.SetValue(m.valueOfNullKey, arrayIndex);

                }
                else
                {
                    values.CopyTo(array, arrayIndex);
                }
            }
            public virtual IEnumerator GetEnumerator()
            {
                IEnumerator e = values.GetEnumerator();
                if (m.hasNullKey)
                {
                    return new HashMapEnumerator(e, m.valueOfNullKey, 2);
                }
                else
                {
                    return e;
                }
            }
            public virtual int Count
            {
                get
                {
                    return values.Count + (m.hasNullKey ? 1 : 0);
                }
            }
            public virtual bool IsSynchronized
            {
                get
                {
                    return values.IsSynchronized;
                }
            }
            public virtual object SyncRoot
            {
                get
                {
                    return values.SyncRoot;
                }
            }
        }
    }
    internal sealed class HproseClassManager
    {

        private static readonly Dictionary<Type, string> classCache1 = new Dictionary<Type, string>();
        private static readonly Dictionary<string, Type> classCache2 = new Dictionary<string, Type>();
        private static readonly object syncRoot = new object();
        public static void Register(Type type, string alias)
        {
            lock (syncRoot)
            {
                if (type != null)
                {
                    classCache1[type] = alias;
                }
                classCache2[alias] = type;
            }
        }
        public static void Register<T>(string alias)
        {
            Register(typeof(T), alias);
        }
        public static string GetClassAlias(Type type)
        {
            lock (syncRoot)
            {

                string alias = null;
                classCache1.TryGetValue(type, out alias);
                return alias;
            }
        }

        public static Type GetClass(string alias)
        {
            lock (syncRoot)
            {

                Type type = null;
                classCache2.TryGetValue(alias, out type);
                return type;
            }
        }

        public static bool ContainsClass(string alias)
        {
            lock (syncRoot)
            {

                return classCache2.ContainsKey(alias);
            }
        }
    }
    internal class HproseException : IOException
    {
        public HproseException()
            : base()
        {
        }
        public HproseException(string message)
            : base(message)
        {
        }
        public HproseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
    internal interface IGListReader
    {
        object ReadList(HproseReader reader);
    }
    internal class GListReader<T> : IGListReader
    {
        public object ReadList(HproseReader reader)
        {
            return reader.ReadList<T>();
        }
    }
    internal interface IGQueueReader
    {
        object ReadQueue(HproseReader reader);
    }
    internal class GQueueReader<T> : IGQueueReader
    {
        public object ReadQueue(HproseReader reader)
        {
            return reader.ReadQueue<T>();
        }
    }
    internal interface IGStackReader
    {
        object ReadStack(HproseReader reader);
    }
    internal class GStackReader<T> : IGStackReader
    {
        public object ReadStack(HproseReader reader)
        {
            return reader.ReadStack<T>();
        }
    }
    internal interface IGIListReader
    {
        object ReadIList(HproseReader reader, Type type);
    }
    internal class GIListReader<T> : IGIListReader
    {
        public object ReadIList(HproseReader reader, Type type)
        {
            return reader.ReadIList<T>(type);
        }
    }
    internal interface IGICollectionReader
    {
        object ReadICollection(HproseReader reader, Type type);
    }
    internal class GICollectionReader<T> : IGICollectionReader
    {
        public object ReadICollection(HproseReader reader, Type type)
        {
            return reader.ReadICollection<T>(type);
        }
    }
    internal interface IGDictionaryReader
    {
        object ReadDictionary(HproseReader reader);
    }
    internal class GDictionaryReader<TKey, TValue> : IGDictionaryReader
    {
        public object ReadDictionary(HproseReader reader)
        {
            return reader.ReadDictionary<TKey, TValue>();
        }
    }
    internal interface IGIDictionaryReader
    {
        object ReadIDictionary(HproseReader reader, Type type);
    }
    internal class GIDictionaryReader<TKey, TValue> : IGIDictionaryReader
    {
        public object ReadIDictionary(HproseReader reader, Type type)
        {
            return reader.ReadIDictionary<TKey, TValue>(type);
        }
    }
    internal struct FieldTypeInfo
    {
        public FieldInfo info;
        public Type type;
        public TypeEnum typeEnum;
        public FieldTypeInfo(FieldInfo field)
        {
            info = field;
            type = field.FieldType;
            typeEnum = HproseHelper.GetTypeEnum(type);
        }
    }
    internal struct PropertyTypeInfo
    {
        public PropertyInfo info;
        public Type type;
        public TypeEnum typeEnum;
        public PropertyTypeInfo(PropertyInfo property)
        {
            info = property;
            type = property.PropertyType;
            typeEnum = HproseHelper.GetTypeEnum(type);
        }
    }
    internal struct MemberTypeInfo
    {
        public MemberInfo info;
        public Type type;
        public TypeEnum typeEnum;
        public MemberTypeInfo(MemberInfo member)
        {
            info = member;
            type = (member is FieldInfo) ? ((FieldInfo)member).FieldType : ((PropertyInfo)member).PropertyType;
            typeEnum = HproseHelper.GetTypeEnum(type);
        }
    }
    internal sealed class HproseHelper
    {
        private static readonly UTF8Encoding utf8Encoding = new UTF8Encoding();
        public static readonly Type typeofArrayList = typeof(ArrayList);
        public static readonly Type typeofQueue = typeof(Queue);
        public static readonly Type typeofStack = typeof(Stack);
        public static readonly Type typeofHashMap = typeof(HashMap);
        public static readonly Type typeofHashtable = typeof(Hashtable);
        public static readonly Type typeofDBNull = typeof(DBNull);
        public static readonly Type typeofBitArray = typeof(BitArray);
        public static readonly Type typeofBoolean = typeof(Boolean);
        public static readonly Type typeofBooleanArray = typeof(Boolean[]);
        public static readonly Type typeofBigInteger = typeof(BigInteger);
        public static readonly Type typeofBigIntegerArray = typeof(BigInteger[]);
        public static readonly Type typeofByte = typeof(Byte);
        public static readonly Type typeofByteArray = typeof(Byte[]);
        public static readonly Type typeofBytesArray = typeof(Byte[][]);
        public static readonly Type typeofChar = typeof(Char);
        public static readonly Type typeofCharArray = typeof(Char[]);
        public static readonly Type typeofCharsArray = typeof(Char[][]);
        public static readonly Type typeofDateTime = typeof(DateTime);
        public static readonly Type typeofDateTimeArray = typeof(DateTime[]);
        public static readonly Type typeofDecimal = typeof(Decimal);
        public static readonly Type typeofDecimalArray = typeof(Decimal[]);
        public static readonly Type typeofDouble = typeof(Double);
        public static readonly Type typeofDoubleArray = typeof(Double[]);
        public static readonly Type typeofGuid = typeof(Guid);
        public static readonly Type typeofGuidArray = typeof(Guid[]);
        public static readonly Type typeofICollection = typeof(ICollection);
        public static readonly Type typeofIDictionary = typeof(IDictionary);
        public static readonly Type typeofIList = typeof(IList);
        public static readonly Type typeofInt16 = typeof(Int16);
        public static readonly Type typeofInt16Array = typeof(Int16[]);
        public static readonly Type typeofInt32 = typeof(Int32);
        public static readonly Type typeofInt32Array = typeof(Int32[]);
        public static readonly Type typeofInt64 = typeof(Int64);
        public static readonly Type typeofInt64Array = typeof(Int64[]);
        public static readonly Type typeofMemoryStream = typeof(MemoryStream);
        public static readonly Type typeofObject = typeof(Object);
        public static readonly Type typeofObjectArray = typeof(Object[]);
        public static readonly Type typeofSByte = typeof(SByte);
        public static readonly Type typeofSByteArray = typeof(SByte[]);
        public static readonly Type typeofSingle = typeof(Single);
        public static readonly Type typeofSingleArray = typeof(Single[]);
        public static readonly Type typeofStream = typeof(Stream);
        public static readonly Type typeofString = typeof(String);
        public static readonly Type typeofStringArray = typeof(String[]);
        public static readonly Type typeofStringBuilder = typeof(StringBuilder);
        public static readonly Type typeofStringBuilderArray = typeof(StringBuilder[]);
        public static readonly Type typeofTimeSpan = typeof(TimeSpan);
        public static readonly Type typeofTimeSpanArray = typeof(TimeSpan[]);
        public static readonly Type typeofUInt16 = typeof(UInt16);
        public static readonly Type typeofUInt16Array = typeof(UInt16[]);
        public static readonly Type typeofUInt32 = typeof(UInt32);
        public static readonly Type typeofUInt32Array = typeof(UInt32[]);
        public static readonly Type typeofUInt64 = typeof(UInt64);
        public static readonly Type typeofUInt64Array = typeof(UInt64[]);
        public static readonly Type typeofIntPtr = typeof(IntPtr);
        public static readonly Type typeofIntPtrArray = typeof(IntPtr[]);
        public static readonly Type typeofNullableBoolean = typeof(Nullable<Boolean>);
        public static readonly Type typeofNullableChar = typeof(Nullable<Char>);
        public static readonly Type typeofNullableSByte = typeof(Nullable<SByte>);
        public static readonly Type typeofNullableByte = typeof(Nullable<Byte>);
        public static readonly Type typeofNullableInt16 = typeof(Nullable<Int16>);
        public static readonly Type typeofNullableUInt16 = typeof(Nullable<UInt16>);
        public static readonly Type typeofNullableInt32 = typeof(Nullable<Int32>);
        public static readonly Type typeofNullableUInt32 = typeof(Nullable<UInt32>);
        public static readonly Type typeofNullableInt64 = typeof(Nullable<Int64>);
        public static readonly Type typeofNullableUInt64 = typeof(Nullable<UInt64>);
        public static readonly Type typeofNullableIntPtr = typeof(Nullable<IntPtr>);
        public static readonly Type typeofNullableSingle = typeof(Nullable<Single>);
        public static readonly Type typeofNullableDouble = typeof(Nullable<Double>);
        public static readonly Type typeofNullableDecimal = typeof(Nullable<Decimal>);
        public static readonly Type typeofNullableDateTime = typeof(Nullable<DateTime>);
        public static readonly Type typeofNullableGuid = typeof(Nullable<Guid>);
        public static readonly Type typeofNullableBigInteger = typeof(Nullable<BigInteger>);
        public static readonly Type typeofNullableTimeSpan = typeof(Nullable<TimeSpan>);

        public static readonly Type typeofObjectList = typeof(List<Object>);
        public static readonly Type typeofBooleanList = typeof(List<Boolean>);
        public static readonly Type typeofCharList = typeof(List<Char>);
        public static readonly Type typeofSByteList = typeof(List<SByte>);
        public static readonly Type typeofByteList = typeof(List<Byte>);
        public static readonly Type typeofInt16List = typeof(List<Int16>);
        public static readonly Type typeofUInt16List = typeof(List<UInt16>);
        public static readonly Type typeofInt32List = typeof(List<Int32>);
        public static readonly Type typeofUInt32List = typeof(List<UInt32>);
        public static readonly Type typeofInt64List = typeof(List<Int64>);
        public static readonly Type typeofUInt64List = typeof(List<UInt64>);
        public static readonly Type typeofIntPtrList = typeof(List<IntPtr>);
        public static readonly Type typeofSingleList = typeof(List<Single>);
        public static readonly Type typeofDoubleList = typeof(List<Double>);
        public static readonly Type typeofDecimalList = typeof(List<Decimal>);
        public static readonly Type typeofDateTimeList = typeof(List<DateTime>);
        public static readonly Type typeofStringList = typeof(List<String>);
        public static readonly Type typeofStringBuilderList = typeof(List<StringBuilder>);
        public static readonly Type typeofGuidList = typeof(List<Guid>);
        public static readonly Type typeofBigIntegerList = typeof(List<BigInteger>);
        public static readonly Type typeofTimeSpanList = typeof(List<TimeSpan>);
        public static readonly Type typeofCharsList = typeof(List<Char[]>);
        public static readonly Type typeofBytesList = typeof(List<Byte[]>);

        public static readonly Type typeofObjectIList = typeof(IList<Object>);
        public static readonly Type typeofBooleanIList = typeof(IList<Boolean>);
        public static readonly Type typeofCharIList = typeof(IList<Char>);
        public static readonly Type typeofSByteIList = typeof(IList<SByte>);
        public static readonly Type typeofByteIList = typeof(IList<Byte>);
        public static readonly Type typeofInt16IList = typeof(IList<Int16>);
        public static readonly Type typeofUInt16IList = typeof(IList<UInt16>);
        public static readonly Type typeofInt32IList = typeof(IList<Int32>);
        public static readonly Type typeofUInt32IList = typeof(IList<UInt32>);
        public static readonly Type typeofInt64IList = typeof(IList<Int64>);
        public static readonly Type typeofUInt64IList = typeof(IList<UInt64>);
        public static readonly Type typeofIntPtrIList = typeof(IList<IntPtr>);
        public static readonly Type typeofSingleIList = typeof(IList<Single>);
        public static readonly Type typeofDoubleIList = typeof(IList<Double>);
        public static readonly Type typeofDecimalIList = typeof(IList<Decimal>);
        public static readonly Type typeofDateTimeIList = typeof(IList<DateTime>);
        public static readonly Type typeofStringIList = typeof(IList<String>);
        public static readonly Type typeofStringBuilderIList = typeof(IList<StringBuilder>);
        public static readonly Type typeofGuidIList = typeof(IList<Guid>);
        public static readonly Type typeofBigIntegerIList = typeof(IList<BigInteger>);
        public static readonly Type typeofTimeSpanIList = typeof(IList<TimeSpan>);
        public static readonly Type typeofCharsIList = typeof(IList<Char[]>);
        public static readonly Type typeofBytesIList = typeof(IList<Byte[]>);

        public static readonly Type typeofStringObjectHashMap = typeof(HashMap<string, object>);
        public static readonly Type typeofObjectObjectHashMap = typeof(HashMap<object, object>);
        public static readonly Type typeofIntObjectHashMap = typeof(HashMap<int, object>);
        public static readonly Type typeofStringObjectDictionary = typeof(Dictionary<string, object>);
        public static readonly Type typeofObjectObjectDictionary = typeof(Dictionary<object, object>);
        public static readonly Type typeofIntObjectDictionary = typeof(Dictionary<int, object>);

        public static readonly Type typeofDictionary = typeof(Dictionary<,>);
        public static readonly Type typeofList = typeof(List<>);
        public static readonly Type typeofGQueue = typeof(Queue<>);
        public static readonly Type typeofGStack = typeof(Stack<>);
        public static readonly Type typeofGHashMap = typeof(HashMap<,>);
        public static readonly Type typeofGICollection = typeof(ICollection<>);
        public static readonly Type typeofGIDictionary = typeof(IDictionary<,>);
        public static readonly Type typeofGIList = typeof(IList<>);
        public static readonly Type typeofIgnoreDataMember = typeof(IgnoreDataMemberAttribute);
        public static readonly Type typeofDataContract = typeof(DataContractAttribute);
        public static readonly Type typeofDataMember = typeof(DataMemberAttribute);
        public static readonly Type typeofISerializable = typeof(ISerializable);

        public static readonly Type typeofIpAddress = typeof(IPAddress);
        internal static readonly Dictionary<Type, TypeEnum> typeMap = new Dictionary<Type, TypeEnum>();

        static HproseHelper()
        {
            typeMap[typeofDBNull] = TypeEnum.DBNull;
            typeMap[typeofObject] = TypeEnum.Object;
            typeMap[typeofBoolean] = TypeEnum.Boolean;
            typeMap[typeofChar] = TypeEnum.Char;
            typeMap[typeofSByte] = TypeEnum.SByte;
            typeMap[typeofByte] = TypeEnum.Byte;
            typeMap[typeofInt16] = TypeEnum.Int16;
            typeMap[typeofUInt16] = TypeEnum.UInt16;
            typeMap[typeofInt32] = TypeEnum.Int32;
            typeMap[typeofUInt32] = TypeEnum.UInt32;
            typeMap[typeofInt64] = TypeEnum.Int64;
            typeMap[typeofUInt64] = TypeEnum.UInt64;
            typeMap[typeofIntPtr] = TypeEnum.IntPtr;
            typeMap[typeofSingle] = TypeEnum.Single;
            typeMap[typeofDouble] = TypeEnum.Double;
            typeMap[typeofDecimal] = TypeEnum.Decimal;
            typeMap[typeofDateTime] = TypeEnum.DateTime;
            typeMap[typeofString] = TypeEnum.String;
            typeMap[typeofBigInteger] = TypeEnum.BigInteger;
            typeMap[typeofGuid] = TypeEnum.Guid;
            typeMap[typeofStringBuilder] = TypeEnum.StringBuilder;
            typeMap[typeofTimeSpan] = TypeEnum.TimeSpan;

            typeMap[typeofObjectArray] = TypeEnum.ObjectArray;
            typeMap[typeofBooleanArray] = TypeEnum.BooleanArray;
            typeMap[typeofCharArray] = TypeEnum.CharArray;
            typeMap[typeofSByteArray] = TypeEnum.SByteArray;
            typeMap[typeofByteArray] = TypeEnum.ByteArray;
            typeMap[typeofInt16Array] = TypeEnum.Int16Array;
            typeMap[typeofUInt16Array] = TypeEnum.UInt16Array;
            typeMap[typeofInt32Array] = TypeEnum.Int32Array;
            typeMap[typeofUInt32Array] = TypeEnum.UInt32Array;
            typeMap[typeofInt64Array] = TypeEnum.Int64Array;
            typeMap[typeofUInt64Array] = TypeEnum.UInt64Array;
            typeMap[typeofIntPtrArray] = TypeEnum.IntPtrArray;
            typeMap[typeofSingleArray] = TypeEnum.SingleArray;
            typeMap[typeofDoubleArray] = TypeEnum.DoubleArray;
            typeMap[typeofDecimalArray] = TypeEnum.DecimalArray;
            typeMap[typeofDateTimeArray] = TypeEnum.DateTimeArray;
            typeMap[typeofStringArray] = TypeEnum.StringArray;
            typeMap[typeofBigIntegerArray] = TypeEnum.BigIntegerArray;
            typeMap[typeofGuidArray] = TypeEnum.GuidArray;
            typeMap[typeofStringBuilderArray] = TypeEnum.StringBuilderArray;
            typeMap[typeofTimeSpanArray] = TypeEnum.TimeSpanArray;
            typeMap[typeofBytesArray] = TypeEnum.BytesArray;
            typeMap[typeofCharsArray] = TypeEnum.CharsArray;
            typeMap[typeofMemoryStream] = TypeEnum.MemoryStream;
            typeMap[typeofStream] = TypeEnum.Stream;
            typeMap[typeofICollection] = TypeEnum.ICollection;
            typeMap[typeofIDictionary] = TypeEnum.IDictionary;
            typeMap[typeofIList] = TypeEnum.IList;
            typeMap[typeofBitArray] = TypeEnum.BitArray;
            typeMap[typeofArrayList] = TypeEnum.ArrayList;
            typeMap[typeofHashMap] = TypeEnum.HashMap;
            typeMap[typeofHashtable] = TypeEnum.Hashtable;
            typeMap[typeofQueue] = TypeEnum.Queue;
            typeMap[typeofStack] = TypeEnum.Stack;
            typeMap[typeofNullableBoolean] = TypeEnum.NullableBoolean;
            typeMap[typeofNullableChar] = TypeEnum.NullableChar;
            typeMap[typeofNullableSByte] = TypeEnum.NullableSByte;
            typeMap[typeofNullableByte] = TypeEnum.NullableByte;
            typeMap[typeofNullableInt16] = TypeEnum.NullableInt16;
            typeMap[typeofNullableUInt16] = TypeEnum.NullableUInt16;
            typeMap[typeofNullableInt32] = TypeEnum.NullableInt32;
            typeMap[typeofNullableUInt32] = TypeEnum.NullableUInt32;
            typeMap[typeofNullableInt64] = TypeEnum.NullableInt64;
            typeMap[typeofNullableUInt64] = TypeEnum.NullableUInt64;
            typeMap[typeofNullableIntPtr] = TypeEnum.NullableIntPtr;
            typeMap[typeofNullableSingle] = TypeEnum.NullableSingle;
            typeMap[typeofNullableDouble] = TypeEnum.NullableDouble;
            typeMap[typeofNullableDecimal] = TypeEnum.NullableDecimal;
            typeMap[typeofNullableDateTime] = TypeEnum.NullableDateTime;
            typeMap[typeofNullableBigInteger] = TypeEnum.NullableBigInteger;
            typeMap[typeofNullableGuid] = TypeEnum.NullableGuid;
            typeMap[typeofNullableTimeSpan] = TypeEnum.NullableTimeSpan;

            typeMap[typeofObjectList] = TypeEnum.ObjectList;
            typeMap[typeofBooleanList] = TypeEnum.BooleanList;
            typeMap[typeofCharList] = TypeEnum.CharList;
            typeMap[typeofSByteList] = TypeEnum.SByteList;
            typeMap[typeofByteList] = TypeEnum.ByteList;
            typeMap[typeofInt16List] = TypeEnum.Int16List;
            typeMap[typeofUInt16List] = TypeEnum.UInt16List;
            typeMap[typeofInt32List] = TypeEnum.Int32List;
            typeMap[typeofUInt32List] = TypeEnum.UInt32List;
            typeMap[typeofInt64List] = TypeEnum.Int64List;
            typeMap[typeofUInt64List] = TypeEnum.UInt64List;
            typeMap[typeofIntPtrList] = TypeEnum.IntPtrList;
            typeMap[typeofSingleList] = TypeEnum.SingleList;
            typeMap[typeofDoubleList] = TypeEnum.DoubleList;
            typeMap[typeofDecimalList] = TypeEnum.DecimalList;
            typeMap[typeofDateTimeList] = TypeEnum.DateTimeList;
            typeMap[typeofStringList] = TypeEnum.StringList;
            typeMap[typeofBigIntegerList] = TypeEnum.BigIntegerList;
            typeMap[typeofGuidList] = TypeEnum.GuidList;
            typeMap[typeofStringBuilderList] = TypeEnum.StringBuilderList;
            typeMap[typeofTimeSpanList] = TypeEnum.TimeSpanList;
            typeMap[typeofBytesList] = TypeEnum.BytesList;
            typeMap[typeofCharsList] = TypeEnum.CharsList;

            typeMap[typeofObjectIList] = TypeEnum.ObjectIList;
            typeMap[typeofBooleanIList] = TypeEnum.BooleanIList;
            typeMap[typeofCharIList] = TypeEnum.CharIList;
            typeMap[typeofSByteIList] = TypeEnum.SByteIList;
            typeMap[typeofByteIList] = TypeEnum.ByteIList;
            typeMap[typeofInt16IList] = TypeEnum.Int16IList;
            typeMap[typeofUInt16IList] = TypeEnum.UInt16IList;
            typeMap[typeofInt32IList] = TypeEnum.Int32IList;
            typeMap[typeofUInt32IList] = TypeEnum.UInt32IList;
            typeMap[typeofInt64IList] = TypeEnum.Int64IList;
            typeMap[typeofUInt64IList] = TypeEnum.UInt64IList;
            typeMap[typeofIntPtrIList] = TypeEnum.IntPtrIList;
            typeMap[typeofSingleIList] = TypeEnum.SingleIList;
            typeMap[typeofDoubleIList] = TypeEnum.DoubleIList;
            typeMap[typeofDecimalIList] = TypeEnum.DecimalIList;
            typeMap[typeofDateTimeIList] = TypeEnum.DateTimeIList;
            typeMap[typeofStringIList] = TypeEnum.StringIList;
            typeMap[typeofBigIntegerIList] = TypeEnum.BigIntegerIList;
            typeMap[typeofGuidIList] = TypeEnum.GuidIList;
            typeMap[typeofStringBuilderIList] = TypeEnum.StringBuilderIList;
            typeMap[typeofTimeSpanIList] = TypeEnum.TimeSpanIList;
            typeMap[typeofBytesIList] = TypeEnum.BytesIList;
            typeMap[typeofCharsIList] = TypeEnum.CharsIList;

            typeMap[typeofStringObjectHashMap] = TypeEnum.StringObjectHashMap;
            typeMap[typeofObjectObjectHashMap] = TypeEnum.ObjectObjectHashMap;
            typeMap[typeofIntObjectHashMap] = TypeEnum.IntObjectHashMap;
            typeMap[typeofStringObjectDictionary] = TypeEnum.StringObjectDictionary;
            typeMap[typeofObjectObjectDictionary] = TypeEnum.ObjectObjectDictionary;
            typeMap[typeofIntObjectDictionary] = TypeEnum.IntObjectDictionary;

            typeMap[typeofIpAddress] = TypeEnum.IpAddress;
        }

        internal static bool IsInstantiableClass(Type type)
        {

            return !type.IsInterface && !type.IsAbstract;
        }

        private static bool IsGenericList(Type type)
        {
            return (type.GetGenericTypeDefinition() == typeofList);
        }

        private static bool IsGenericDictionary(Type type)
        {
            return (type.GetGenericTypeDefinition() == typeofDictionary);
        }

        private static bool IsGenericQueue(Type type)
        {
            return (type.GetGenericTypeDefinition() == typeofGQueue);
        }

        private static bool IsGenericStack(Type type)
        {
            return (type.GetGenericTypeDefinition() == typeofGStack);
        }

        private static bool IsGenericHashMap(Type type)
        {
            return (type.GetGenericTypeDefinition() == typeofGHashMap);
        }

        private static bool IsGenericIList(Type type)
        {

            Type[] args = type.GetGenericArguments();
            return (args.Length == 1 && IsAssignableFrom(typeofGIList.MakeGenericType(args), type));
        }

        private static bool IsGenericIDictionary(Type type)
        {

            Type[] args = type.GetGenericArguments();
            return (args.Length == 2 && IsAssignableFrom(typeofGIDictionary.MakeGenericType(args), type));
        }

        private static bool IsGenericICollection(Type type)
        {

            Type[] args = type.GetGenericArguments();
            return (args.Length == 1 && IsAssignableFrom(typeofGICollection.MakeGenericType(args), type));
        }

        internal static TypeEnum GetArrayTypeEnum(Type type)
        {
            TypeEnum t;
            if (typeMap.TryGetValue(type, out t)) return t;

            return TypeEnum.OtherTypeArray;
        }

        internal static bool IsAssignableFrom(Type t, Type type)
        {

            return t.IsAssignableFrom(type);
        }

        internal static TypeEnum GetTypeEnum(Type type)
        {
            if (type == null) return TypeEnum.Null;
            TypeEnum t;
            if (typeMap.TryGetValue(type, out t)) return t;


            if (type.IsArray) return TypeEnum.OtherTypeArray;
            if (type.IsByRef) return GetTypeEnum(type.GetElementType());
            if (type.IsEnum) return TypeEnum.Enum;

            if (type.IsGenericType)
            {
                if (IsGenericDictionary(type)) return TypeEnum.GenericDictionary;
                if (IsGenericList(type)) return TypeEnum.GenericList;
                if (IsGenericQueue(type)) return TypeEnum.GenericQueue;
                if (IsGenericStack(type)) return TypeEnum.GenericStack;
                if (IsGenericHashMap(type)) return TypeEnum.GenericHashMap;
                if (IsGenericIDictionary(type)) return TypeEnum.GenericIDictionary;
                if (IsGenericIList(type)) return TypeEnum.GenericIList;
                if (IsGenericICollection(type)) return TypeEnum.GenericICollection;
            }
            if (IsInstantiableClass(type))
            {

                if (IsAssignableFrom(typeofIDictionary, type)) return TypeEnum.Dictionary;
                if (IsAssignableFrom(typeofIList, type)) return TypeEnum.List;
            }
            if (typeofISerializable.IsAssignableFrom(type)) return TypeEnum.UnSupportedType;
            return TypeEnum.OtherType;
        }

        private static readonly Dictionary<Type, Dictionary<string, FieldTypeInfo>> fieldsCache = new Dictionary<Type, Dictionary<string, FieldTypeInfo>>();
        private static readonly Dictionary<Type, Dictionary<string, PropertyTypeInfo>> propertiesCache = new Dictionary<Type, Dictionary<string, PropertyTypeInfo>>();
        private static readonly Dictionary<Type, Dictionary<string, MemberTypeInfo>> membersCache = new Dictionary<Type, Dictionary<string, MemberTypeInfo>>();
        private static readonly Dictionary<Type, IGListReader> gListReaderCache = new Dictionary<Type, IGListReader>();
        private static readonly Dictionary<Type, IGQueueReader> gQueueReaderCache = new Dictionary<Type, IGQueueReader>();
        private static readonly Dictionary<Type, IGStackReader> gStackReaderCache = new Dictionary<Type, IGStackReader>();
        private static readonly Dictionary<Type, IGIListReader> gIListReaderCache = new Dictionary<Type, IGIListReader>();
        private static readonly Dictionary<Type, IGICollectionReader> gICollectionReaderCache = new Dictionary<Type, IGICollectionReader>();
        private static readonly Dictionary<Type, IGDictionaryReader> gDictionaryReaderCache = new Dictionary<Type, IGDictionaryReader>();
        private static readonly Dictionary<Type, IGIDictionaryReader> gIDictionaryReaderCache = new Dictionary<Type, IGIDictionaryReader>();



        private static readonly Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();

        public static bool IsSerializable(Type type)
        {

            return type.IsSerializable;
        }

        private static Dictionary<string, MemberTypeInfo> GetMembersWithoutCache(Type type)
        {


            if (type.IsDefined(typeofDataContract, false))
            {
                return GetDataMembersWithoutCache(type);
            }
            Dictionary<string, MemberTypeInfo> members = new Dictionary<string, MemberTypeInfo>(StringComparer.OrdinalIgnoreCase);

            FieldAttributes ns = FieldAttributes.NotSerialized;

            BindingFlags bindingflags = BindingFlags.Public |
                                        BindingFlags.Instance;
            PropertyInfo[] piarray = type.GetProperties(bindingflags);
            foreach (PropertyInfo pi in piarray)
            {
                string name;
                if (pi.CanRead && pi.CanWrite &&
                    !pi.IsDefined(typeofIgnoreDataMember, false) &&
                    pi.GetIndexParameters().GetLength(0) == 0 &&
                    !members.ContainsKey(name = pi.Name))
                {
                    name = char.ToLower(name[0]) + name.Substring(1);
                    members[name] = new MemberTypeInfo(pi);
                }
            }
            bindingflags = BindingFlags.Public | BindingFlags.Instance;
            FieldInfo[] fiarray = type.GetFields(bindingflags);
            foreach (FieldInfo fi in fiarray)
            {
                string name;
                if (!fi.IsDefined(typeofIgnoreDataMember, false) &&
                    ((fi.Attributes & ns) != ns) &&
                    !members.ContainsKey(name = fi.Name))
                {
                    name = char.ToLower(name[0]) + name.Substring(1);
                    members[name] = new MemberTypeInfo(fi);
                }
            }
            return members;
        }

        private static Dictionary<string, MemberTypeInfo> GetDataMembersWithoutCache(Type type)
        {
            Dictionary<string, MemberTypeInfo> members = new Dictionary<string, MemberTypeInfo>(StringComparer.OrdinalIgnoreCase);
            BindingFlags bindingflags = BindingFlags.Public |
                                        BindingFlags.NonPublic |
                                        BindingFlags.Instance;
            PropertyInfo[] piarray = type.GetProperties(bindingflags);
            foreach (PropertyInfo pi in piarray)
            {
                string name;
                if (pi.CanRead && pi.CanWrite &&
                    pi.IsDefined(typeofDataMember, false) &&
                    !pi.IsDefined(typeofIgnoreDataMember, false) &&
                    pi.GetIndexParameters().GetLength(0) == 0 &&
                    !members.ContainsKey(name = pi.Name))
                {
                    name = char.ToLower(name[0]) + name.Substring(1);
                    members[name] = new MemberTypeInfo(pi);
                }
            }
            FieldInfo[] fiarray = type.GetFields(bindingflags);
            FieldAttributes ns = FieldAttributes.NotSerialized;
            foreach (FieldInfo fi in fiarray)
            {
                string name;
                if (fi.IsDefined(typeofDataMember, false) &&
                    !fi.IsDefined(typeofIgnoreDataMember, false) &&
                    ((fi.Attributes & ns) != ns) &&
                    !members.ContainsKey(name = fi.Name))
                {
                    name = char.ToLower(name[0]) + name.Substring(1);
                    members[name] = new MemberTypeInfo(fi);
                }
            }
            return members;
        }

        internal static Dictionary<string, MemberTypeInfo> GetMembers(Type type)
        {

            ICollection pc = membersCache;
            lock (pc.SyncRoot)
            {
                Dictionary<string, MemberTypeInfo> result;
                if (membersCache.TryGetValue(type, out result))
                {

                    return result;
                }
            }
            Dictionary<string, MemberTypeInfo> members;

            members = GetMembersWithoutCache(type);
            lock (pc.SyncRoot)
            {
                membersCache[type] = members;
            }
            return members;
        }

        private static Dictionary<string, PropertyTypeInfo> GetPropertiesWithoutCache(Type type)
        {

            Dictionary<string, PropertyTypeInfo> properties = new Dictionary<string, PropertyTypeInfo>(StringComparer.OrdinalIgnoreCase);

            if (IsSerializable(type))
            {

                BindingFlags bindingflags = BindingFlags.Public |
                                            BindingFlags.Instance;
                PropertyInfo[] piarray = type.GetProperties(bindingflags);
                foreach (PropertyInfo pi in piarray)
                {
                    string name;
                    if (pi.CanRead && pi.CanWrite &&
                        !pi.IsDefined(typeofIgnoreDataMember, false) &&
                        pi.GetIndexParameters().GetLength(0) == 0 &&
                        !properties.ContainsKey(name = pi.Name))
                    {
                        name = char.ToLower(name[0]) + name.Substring(1);
                        properties[name] = new PropertyTypeInfo(pi);
                    }
                }
            }
            return properties;
        }

        internal static Dictionary<string, PropertyTypeInfo> GetProperties(Type type)
        {

            ICollection pc = propertiesCache;
            lock (pc.SyncRoot)
            {
                Dictionary<string, PropertyTypeInfo> result;
                if (propertiesCache.TryGetValue(type, out result))
                {

                    return result;
                }
            }
            Dictionary<string, PropertyTypeInfo> properties;

            properties = GetPropertiesWithoutCache(type);
            lock (pc.SyncRoot)
            {
                propertiesCache[type] = properties;
            }
            return properties;
        }

        private static Dictionary<string, FieldTypeInfo> GetFieldsWithoutCache(Type type)
        {

            Dictionary<string, FieldTypeInfo> fields = new Dictionary<string, FieldTypeInfo>(StringComparer.OrdinalIgnoreCase);

            FieldAttributes ns = FieldAttributes.NotSerialized;

            BindingFlags bindingflags = BindingFlags.Public |
                                        BindingFlags.NonPublic |
                                        BindingFlags.DeclaredOnly |
                                        BindingFlags.Instance;
            while (type != typeofObject && IsSerializable(type))
            {
                FieldInfo[] fiarray = type.GetFields(bindingflags);
                foreach (FieldInfo fi in fiarray)
                {
                    string name;
                    if (((fi.Attributes & ns) != ns) &&
                        !fi.IsDefined(typeofIgnoreDataMember, false) &&
                        !fields.ContainsKey(name = fi.Name))
                    {
                        name = char.ToLower(name[0]) + name.Substring(1);

                        fields[name] = new FieldTypeInfo(fi);
                    }
                }
                type = type.BaseType;
            }
            return fields;
        }

        internal static Dictionary<string, FieldTypeInfo> GetFields(Type type)
        {

            ICollection fc = fieldsCache;
            lock (fc.SyncRoot)
            {
                Dictionary<string, FieldTypeInfo> result;
                if (fieldsCache.TryGetValue(type, out result))
                {

                    return result;
                }
            }
            Dictionary<string, FieldTypeInfo> fields;

            fields = GetFieldsWithoutCache(type);
            lock (fc.SyncRoot)
            {
                fieldsCache[type] = fields;
            }
            return fields;
        }

        public static string GetClassName(Type type)
        {
            string className = HproseClassManager.GetClassAlias(type);
            if (className == null)
            {

                className = type.FullName.Replace('.', '_').Replace('+', '_');
                int index = className.IndexOf('`');
                if (index > 0)
                {
                    className = className.Substring(0, index);
                }
                HproseClassManager.Register(type, className);
            }
            return className;
        }

        private static Type GetType(String name)
        {
            Type type = null;
            for (int i = assemblies.Length - 1; type == null && i >= 0; --i)
            {
                type = assemblies[i].GetType(name);
            }
            return type;
        }
        private static Type GetNestedType(StringBuilder name, List<int> positions, int i, char c)
        {

            int length = positions.Count;
            Type type;
            if (i < length)
            {
                name[positions[i++]] = c;

                type = GetNestedType(name, positions, i, '_');
                if (i < length && type == null)
                {
                    type = GetNestedType(name, positions, i, '+');
                }
            }
            else
            {
                type = GetType(name.ToString());
            }
            return type;
        }

        private static Type GetType(StringBuilder name, List<int> positions, int i, char c)
        {

            int length = positions.Count;
            Type type;
            if (i < length)
            {
                name[positions[i++]] = c;

                type = GetType(name, positions, i, '.');
                if (i < length)
                {
                    if (type == null)
                    {
                        type = GetType(name, positions, i, '_');
                    }
                    if (type == null)
                    {
                        type = GetNestedType(name, positions, i, '+');
                    }
                }
            }
            else
            {
                type = GetType(name.ToString());
            }
            return type;
        }
        public static Type GetClass(string className)
        {
            if (HproseClassManager.ContainsClass(className))
            {
                return HproseClassManager.GetClass(className);
            }
            List<int> positions = new List<int>();

            int pos = className.IndexOf('_');
            while (pos > -1)
            {
                positions.Add(pos);
                pos = className.IndexOf('_', pos + 1);
            }
            Type type;
            if (positions.Count > 0)
            {
                StringBuilder name = new StringBuilder(className);
                type = GetType(name, positions, 0, '.');
                if (type == null)
                {
                    type = GetType(name, positions, 0, '_');
                }
                if (type == null)
                {
                    type = GetNestedType(name, positions, 0, '+');
                }
            }
            else
            {
                type = GetType(className.ToString());
            }
            HproseClassManager.Register(type, className);
            return type;
        }


        public static object NewInstance(Type type)
        {
            return CtorAccessor.Get(type).NewInstance();
        }

        internal static IGIListReader GetIGIListReader(Type type)
        {
            ICollection cache = gIListReaderCache;
            IGIListReader listReader = null;
            lock (cache.SyncRoot)
            {
                if (gIListReaderCache.TryGetValue(type, out listReader))
                {
                    return listReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            listReader = Activator.CreateInstance(typeof(GIListReader<>).MakeGenericType(args)) as IGIListReader;
            lock (cache.SyncRoot)
            {
                gIListReaderCache[type] = listReader;
            }
            return listReader;
        }

        internal static IGICollectionReader GetIGICollectionReader(Type type)
        {
            ICollection cache = gICollectionReaderCache;
            IGICollectionReader collectionReader = null;
            lock (cache.SyncRoot)
            {
                if (gICollectionReaderCache.TryGetValue(type, out collectionReader))
                {
                    return collectionReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            collectionReader = Activator.CreateInstance(typeof(GICollectionReader<>).MakeGenericType(args)) as IGICollectionReader;
            lock (cache.SyncRoot)
            {
                gICollectionReaderCache[type] = collectionReader;
            }
            return collectionReader;
        }

        internal static IGIDictionaryReader GetIGIDictionaryReader(Type type)
        {
            ICollection cache = gIDictionaryReaderCache;
            IGIDictionaryReader dictionaryReader = null;
            lock (cache.SyncRoot)
            {
                if (gIDictionaryReaderCache.TryGetValue(type, out dictionaryReader))
                {
                    return dictionaryReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            dictionaryReader = Activator.CreateInstance(typeof(GIDictionaryReader<,>).MakeGenericType(args)) as IGIDictionaryReader;
            lock (cache.SyncRoot)
            {
                gIDictionaryReaderCache[type] = dictionaryReader;
            }
            return dictionaryReader;
        }

        internal static IGListReader GetIGListReader(Type type)
        {
            ICollection cache = gListReaderCache;
            IGListReader listReader = null;
            lock (cache.SyncRoot)
            {
                if (gListReaderCache.TryGetValue(type, out listReader))
                {
                    return listReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            listReader = Activator.CreateInstance(typeof(GListReader<>).MakeGenericType(args)) as IGListReader;
            lock (cache.SyncRoot)
            {
                gListReaderCache[type] = listReader;
            }
            return listReader;
        }

        internal static IGDictionaryReader GetIGDictionaryReader(Type type)
        {
            ICollection cache = gDictionaryReaderCache;
            IGDictionaryReader dictionaryReader = null;
            lock (cache.SyncRoot)
            {
                if (gDictionaryReaderCache.TryGetValue(type, out dictionaryReader))
                {
                    return dictionaryReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            dictionaryReader = Activator.CreateInstance(typeof(GDictionaryReader<,>).MakeGenericType(args)) as IGDictionaryReader;
            lock (cache.SyncRoot)
            {
                gDictionaryReaderCache[type] = dictionaryReader;
            }
            return dictionaryReader;
        }

        internal static IGQueueReader GetIGQueueReader(Type type)
        {
            ICollection cache = gQueueReaderCache;
            IGQueueReader queueReader = null;
            lock (cache.SyncRoot)
            {
                if (gQueueReaderCache.TryGetValue(type, out queueReader))
                {
                    return queueReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            queueReader = Activator.CreateInstance(typeof(GQueueReader<>).MakeGenericType(args)) as IGQueueReader;
            lock (cache.SyncRoot)
            {
                gQueueReaderCache[type] = queueReader;
            }
            return queueReader;
        }


        internal static IGStackReader GetIGStackReader(Type type)
        {
            ICollection cache = gStackReaderCache;
            IGStackReader stackReader = null;
            lock (cache.SyncRoot)
            {
                if (gStackReaderCache.TryGetValue(type, out stackReader))
                {
                    return stackReader;
                }
            }

            Type[] args = type.GetGenericArguments();
            stackReader = Activator.CreateInstance(typeof(GStackReader<>).MakeGenericType(args)) as IGStackReader;
            lock (cache.SyncRoot)
            {
                gStackReaderCache[type] = stackReader;
            }
            return stackReader;
        }
        internal static String ReadWrongInfo(MemoryStream istream)
        {

            return utf8Encoding.GetString(istream.ToArray(), 0, (int)istream.Length);
        }



        public static BigInteger ToBigInteger(string value)
        {
            return BigInteger.Parse(value);
        }
        public static bool ToBoolean(string value)
        {
            if (value == null) return false;
            value = value.ToLower();
            string t = bool.TrueString.ToLower();
            string f = bool.FalseString.ToLower();
            if (value == t) return true;
            if (value == f) return false;
            value = value.Trim();
            if (value == t) return true;
            if (value == f) return false;
            throw new FormatException("Bad Boolean Format");
        }

        public static Guid ToGuid(string data)
        {
            int num = 0, a = 0, b = 0, c = 0;
            byte d, e, f, g, h, i, j, k;
            int[] table = new int[]{0,1,2,3,4,5,6,7,8,9,
                                    0,0,0,0,0,0,0,10,11,
                                    12,13,14,15,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,10,11,12,13,14,15};
            try
            {
                for (int n = 0; n < 8; ++n)
                {
                    a = (a << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    b = (b << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    c = (c << 4) | table[data[++num] - '0'];
                }
                ++num;
                d = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                e = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                num += 5;
                f = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                g = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                h = (byte)(table[data[num + 5] - '0'] << 4 | table[data[num + 6] - '0']);
                i = (byte)(table[data[num + 7] - '0'] << 4 | table[data[num + 8] - '0']);
                j = (byte)(table[data[num + 9] - '0'] << 4 | table[data[num + 10] - '0']);
                k = (byte)(table[data[num + 11] - '0'] << 4 | table[data[num + 12] - '0']);
                return new Guid(a, (short)b, (short)c, d, e, f, g, h, i, j, k);
            }
            catch (IndexOutOfRangeException)
            {
                throw new FormatException("Unrecognized Guid Format");
            }
        }

        public static Guid ToGuid(char[] data)
        {
            int num = 0, a = 0, b = 0, c = 0;
            byte d, e, f, g, h, i, j, k;
            int[] table = new int[]{0,1,2,3,4,5,6,7,8,9,
                                    0,0,0,0,0,0,0,10,11,
                                    12,13,14,15,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,10,11,12,13,14,15};
            try
            {
                for (int n = 0; n < 8; ++n)
                {
                    a = (a << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    b = (b << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    c = (c << 4) | table[data[++num] - '0'];
                }
                ++num;
                d = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                e = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                num += 5;
                f = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                g = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                h = (byte)(table[data[num + 5] - '0'] << 4 | table[data[num + 6] - '0']);
                i = (byte)(table[data[num + 7] - '0'] << 4 | table[data[num + 8] - '0']);
                j = (byte)(table[data[num + 9] - '0'] << 4 | table[data[num + 10] - '0']);
                k = (byte)(table[data[num + 11] - '0'] << 4 | table[data[num + 12] - '0']);
                return new Guid(a, (short)b, (short)c, d, e, f, g, h, i, j, k);
            }
            catch (IndexOutOfRangeException)
            {
                throw new FormatException("Unrecognized Guid Format");
            }
        }

        public static Guid ToGuid(byte[] data)
        {
            int num = 0, a = 0, b = 0, c = 0;
            byte d, e, f, g, h, i, j, k;
            int[] table = new int[]{0,1,2,3,4,5,6,7,8,9,
                                    0,0,0,0,0,0,0,10,11,
                                    12,13,14,15,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,0,0,0,0,0,0,0,0,
                                    0,0,10,11,12,13,14,15};
            try
            {
                for (int n = 0; n < 8; ++n)
                {
                    a = (a << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    b = (b << 4) | table[data[++num] - '0'];
                }
                ++num;
                for (int n = 0; n < 4; ++n)
                {
                    c = (c << 4) | table[data[++num] - '0'];
                }
                ++num;
                d = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                e = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                num += 5;
                f = (byte)(table[data[num + 1] - '0'] << 4 | table[data[num + 2] - '0']);
                g = (byte)(table[data[num + 3] - '0'] << 4 | table[data[num + 4] - '0']);
                h = (byte)(table[data[num + 5] - '0'] << 4 | table[data[num + 6] - '0']);
                i = (byte)(table[data[num + 7] - '0'] << 4 | table[data[num + 8] - '0']);
                j = (byte)(table[data[num + 9] - '0'] << 4 | table[data[num + 10] - '0']);
                k = (byte)(table[data[num + 11] - '0'] << 4 | table[data[num + 12] - '0']);
                return new Guid(a, (short)b, (short)c, d, e, f, g, h, i, j, k);
            }
            catch (IndexOutOfRangeException)
            {
                throw new FormatException("Unrecognized Guid Format");
            }
        }
    }
    internal enum HproseMode
    {
        FieldMode, PropertyMode, MemberMode
    }
    internal interface ReaderRefer
    {
        void Set(object obj);
        object Read(int index);
        void Reset();
    }
    internal sealed class FakeReaderRefer : ReaderRefer
    {
        public void Set(object obj) { }
        public object Read(int index)
        {
            throw new HproseException("Unexpected serialize tag '" +
                                       (char)HproseTags.TagRef +
                                       "' in stream");
        }
        public void Reset() { }
    }
    internal sealed class RealReaderRefer : ReaderRefer
    {
        private List<object> references = new List<object>();

        public void Set(object obj)
        {
            references.Add(obj);
        }
        public object Read(int index)
        {
            return references[index];
        }
        public void Reset()
        {
            references.Clear();
        }
    }
    internal sealed class HproseReader
    {
        public Stream stream;
        private HproseMode mode;
        private ReaderRefer refer;
        private List<object> classref = new List<object>();
        private Dictionary<object, string[]> membersref = new Dictionary<object, string[]>();

        public HproseReader(Stream stream)
            : this(stream, HproseMode.MemberMode, false)
        {
        }
        public HproseReader(Stream stream, bool simple)
            : this(stream, HproseMode.MemberMode, simple)
        {
        }
        public HproseReader(Stream stream, HproseMode mode)
            : this(stream, mode, false)
        {
        }
        public HproseReader(Stream stream, HproseMode mode, bool simple)
        {
            this.stream = stream;
            this.mode = mode;
            this.refer = (simple ? new FakeReaderRefer() as ReaderRefer : new RealReaderRefer() as ReaderRefer);
        }

        public HproseException UnexpectedTag(int tag)
        {
            return UnexpectedTag(tag, null);
        }

        public HproseException UnexpectedTag(int tag, string expectTags)
        {
            if (tag == -1)
            {
                return new HproseException("No byte found in stream");
            }
            else if (expectTags == null)
            {
                return new HproseException("Unexpected serialize tag '" + (char)tag + "' in stream");
            }
            else
            {
                return new HproseException("Tag '" + expectTags + "' expected, but '" + (char)tag +
                                          "' found in stream");
            }
        }

        private HproseException CastError(string srctype, Type desttype)
        {
            return new HproseException(srctype + " can't change to " + desttype.FullName);
        }

        private HproseException CastError(object obj, Type type)
        {
            return new HproseException(obj.GetType().FullName + " can't change to " + type.FullName);
        }

        private void CheckTag(int tag, int expectTag)
        {
            if (tag != expectTag) throw UnexpectedTag(tag, new String((char)expectTag, 1));
        }

        public void CheckTag(int expectTag)
        {
            CheckTag(stream.ReadByte(), expectTag);
        }

        private int CheckTags(int tag, string expectTags)
        {
            if (expectTags.IndexOf((char)tag) == -1) throw UnexpectedTag(tag, expectTags);
            return tag;
        }

        public int CheckTags(string expectTags)
        {
            return CheckTags(stream.ReadByte(), expectTags);
        }

        private StringBuilder ReadUntil(int tag)
        {
            StringBuilder sb = new StringBuilder();
            int i = stream.ReadByte();
            while ((i != tag) && (i != -1))
            {
                sb.Append((char)i);
                i = stream.ReadByte();
            }
            return sb;
        }

        private void SkipUntil(int tag)
        {
            int i = stream.ReadByte();
            while ((i != tag) && (i != -1))
            {
                i = stream.ReadByte();
            }
        }

        public int ReadInt(int tag)
        {
            int result = 0;
            int sign = 1;
            int i = stream.ReadByte();
            switch (i)
            {
                case '-':
                    sign = -1;
                    goto case '+';
                case '+':
                    i = stream.ReadByte();
                    break;
            }
            while ((i != tag) && (i != -1))
            {
                result *= 10;
                result += (i - '0') * sign;
                i = stream.ReadByte();
            }
            return result;
        }

        public long ReadLong(int tag)
        {
            long result = 0L;
            long sign = 1L;
            int i = stream.ReadByte();
            switch (i)
            {
                case '-':
                    sign = -1L;
                    goto case '+';
                case '+':
                    i = stream.ReadByte();
                    break;
            }
            while ((i != tag) && (i != -1))
            {
                result *= 10L;
                result += (i - '0') * sign;
                i = stream.ReadByte();
            }
            return result;
        }

        public float ReadIntAsFloat()
        {
            float result = 0.0F;
            float sign = 1.0F;
            int i = stream.ReadByte();
            switch (i)
            {
                case '-':
                    sign = -1.0F;
                    goto case '+';
                case '+':
                    i = stream.ReadByte();
                    break;
            }
            while ((i != HproseTags.TagSemicolon) && (i != -1))
            {
                result *= 10.0F;
                result += (i - '0') * sign;
                i = stream.ReadByte();
            }
            return result;
        }

        public double ReadIntAsDouble()
        {
            double result = 0.0;
            double sign = 1.0;
            int i = stream.ReadByte();
            switch (i)
            {
                case '-':
                    sign = -1.0;
                    goto case '+';
                case '+':
                    i = stream.ReadByte();
                    break;
            }
            while ((i != HproseTags.TagSemicolon) && (i != -1))
            {
                result *= 10.0;
                result += (i - '0') * sign;
                i = stream.ReadByte();
            }
            return result;
        }

        public decimal ReadIntAsDecimal()
        {
            decimal result = 0.0M;
            decimal sign = 1.0M;
            int i = stream.ReadByte();
            switch (i)
            {
                case '-':
                    sign = -1.0M;
                    goto case '+';
                case '+':
                    i = stream.ReadByte();
                    break;
            }
            while ((i != HproseTags.TagSemicolon) && (i != -1))
            {
                result *= 10.0M;
                result += (i - '0') * sign;
                i = stream.ReadByte();
            }
            return result;
        }

        private float ParseFloat(StringBuilder value)
        {
            return ParseFloat(value.ToString());
        }

        private float ParseFloat(String value)
        {
            try
            {
                return float.Parse(value);
            }
            catch (OverflowException)
            {
                return (value[0] == HproseTags.TagNeg) ?
                        float.NegativeInfinity :
                        float.PositiveInfinity;
            }
        }

        private double ParseDouble(StringBuilder value)
        {
            return ParseDouble(value.ToString());
        }

        private double ParseDouble(String value)
        {
            try
            {
                return double.Parse(value);
            }
            catch (OverflowException)
            {
                return (value[0] == HproseTags.TagNeg) ?
                        double.NegativeInfinity :
                        double.PositiveInfinity;
            }
        }

        private char ReadUTF8CharAsChar()
        {
            char u;
            int c = stream.ReadByte();
            switch (c >> 4)
            {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    {
                        // 0xxx xxxx
                        u = (char)c;
                        break;
                    }
                case 12:
                case 13:
                    {
                        // 110x xxxx   10xx xxxx
                        int c2 = stream.ReadByte();
                        u = (char)(((c & 0x1f) << 6) |
                                   (c2 & 0x3f));
                        break;
                    }
                case 14:
                    {
                        // 1110 xxxx  10xx xxxx  10xx xxxx
                        int c2 = stream.ReadByte();
                        int c3 = stream.ReadByte();
                        u = (char)(((c & 0x0f) << 12) |
                                  ((c2 & 0x3f) << 6) |
                                   (c3 & 0x3f));
                        break;
                    }
                default:
                    throw new HproseException("bad utf-8 encoding at " +
                                                  ((c < 0) ? "end of stream" :
                                                  "0x" + (c & 0xff).ToString("x2")));
            }
            return u;
        }

        private char[] ReadChars()
        {
            int count = ReadInt(HproseTags.TagQuote);
            char[] buf = new char[count];
            for (int i = 0; i < count; ++i)
            {
                int c = stream.ReadByte();
                switch (c >> 4)
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        {
                            // 0xxx xxxx
                            buf[i] = (char)c;
                            break;
                        }
                    case 12:
                    case 13:
                        {
                            // 110x xxxx   10xx xxxx
                            int c2 = stream.ReadByte();
                            buf[i] = (char)(((c & 0x1f) << 6) |
                                             (c2 & 0x3f));
                            break;
                        }
                    case 14:
                        {
                            // 1110 xxxx  10xx xxxx  10xx xxxx
                            int c2 = stream.ReadByte();
                            int c3 = stream.ReadByte();
                            buf[i] = (char)(((c & 0x0f) << 12) |
                                             ((c2 & 0x3f) << 6) |
                                             (c3 & 0x3f));
                            break;
                        }
                    case 15:
                        {
                            // 1111 0xxx  10xx xxxx  10xx xxxx  10xx xxxx
                            if ((c & 0xf) <= 4)
                            {
                                int c2 = stream.ReadByte();
                                int c3 = stream.ReadByte();
                                int c4 = stream.ReadByte();
                                int s = ((c & 0x07) << 18) |
                                        ((c2 & 0x3f) << 12) |
                                        ((c3 & 0x3f) << 6) |
                                        (c4 & 0x3f) - 0x10000;
                                if (0 <= s && s <= 0xfffff)
                                {
                                    buf[i] = (char)(((s >> 10) & 0x03ff) | 0xd800);
                                    buf[++i] = (char)((s & 0x03ff) | 0xdc00);
                                    break;
                                }
                            }
                            goto default;
                            // no break here!! here need throw exception.
                        }
                    default:
                        throw new HproseException("bad utf-8 encoding at " +
                                                  ((c < 0) ? "end of stream" :
                                                  "0x" + (c & 0xff).ToString("x2")));
                }
            }
            stream.ReadByte();
            return buf;
        }

        private String ReadCharsAsString()
        {
            return new String(ReadChars());
        }

        private MemoryStream ReadUTF8CharAsStream()
        {
            MemoryStream ms = new MemoryStream();
            int c = stream.ReadByte();
            switch (c >> 4)
            {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    {
                        ms.WriteByte((byte)c);
                        break;
                    }
                case 12:
                case 13:
                    {
                        int c2 = stream.ReadByte();
                        ms.WriteByte((byte)c);
                        ms.WriteByte((byte)c2);
                        break;
                    }
                case 14:
                    {
                        int c2 = stream.ReadByte();
                        int c3 = stream.ReadByte();
                        ms.WriteByte((byte)c);
                        ms.WriteByte((byte)c2);
                        ms.WriteByte((byte)c3);
                        break;
                    }
                default:
                    throw new HproseException("bad utf-8 encoding at " +
                                              ((c < 0) ? "end of stream" :
                                              "0x" + (c & 0xff).ToString("x2")));
            }
            ms.Position = 0;
            return ms;
        }

        private MemoryStream ReadCharsAsStream()
        {
            int count = ReadInt(HproseTags.TagQuote);
            // here count is capacity, not the real size
            MemoryStream ms = new MemoryStream(count * 3);

            for (int i = 0; i < count; ++i)
            {
                int c = stream.ReadByte();
                switch (c >> 4)
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        {
                            ms.WriteByte((byte)c);
                            break;
                        }
                    case 12:
                    case 13:
                        {
                            int c2 = stream.ReadByte();
                            ms.WriteByte((byte)c);
                            ms.WriteByte((byte)c2);
                            break;
                        }
                    case 14:
                        {
                            int c2 = stream.ReadByte();
                            int c3 = stream.ReadByte();
                            ms.WriteByte((byte)c);
                            ms.WriteByte((byte)c2);
                            ms.WriteByte((byte)c3);
                            break;
                        }
                    case 15:
                        {
                            // 1111 0xxx  10xx xxxx  10xx xxxx  10xx xxxx
                            if ((c & 0xf) <= 4)
                            {
                                int c2 = stream.ReadByte();
                                int c3 = stream.ReadByte();
                                int c4 = stream.ReadByte();
                                int s = ((c & 0x07) << 18) |
                                        ((c2 & 0x3f) << 12) |
                                        ((c3 & 0x3f) << 6) |
                                        (c4 & 0x3f) - 0x10000;
                                if (0 <= s && s <= 0xfffff)
                                {
                                    ms.WriteByte((byte)c);
                                    ms.WriteByte((byte)c2);
                                    ms.WriteByte((byte)c3);
                                    ms.WriteByte((byte)c4);
                                    break;
                                }
                            }
                            goto default;
                            // no break here!! here need throw exception.
                        }
                    default:
                        throw new HproseException("bad utf-8 encoding at " +
                                                  ((c < 0) ? "end of stream" :
                                                  "0x" + (c & 0xff).ToString("x2")));
                }
            }
            stream.ReadByte();
            ms.Position = 0;
            refer.Set(ms);
            return ms;
        }

        private MemoryStream ReadBytesAsStream()
        {
            int len = ReadInt(HproseTags.TagQuote);
            int off = 0;
            byte[] b = new byte[len];
            while (len > 0)
            {
                int size = stream.Read(b, off, len);
                off += size;
                len -= size;
            }
            MemoryStream ms = new MemoryStream(b, 0, len, true);

            stream.ReadByte();
            refer.Set(ms);
            return ms;
        }

        private IDictionary ReadObjectAsMap(IDictionary map)
        {
            object c = classref[ReadInt(HproseTags.TagOpenbrace)];
            string[] memberNames = membersref[c];

            refer.Set(map);
            int count = memberNames.Length;
            for (int i = 0; i < count; ++i)
            {
                map[memberNames[i]] = Unserialize();
            }
            stream.ReadByte();
            return map;
        }

        private void ReadMapAsObjectFields(object obj, Type type, int count)
        {
            Dictionary<string, FieldTypeInfo> fields = HproseHelper.GetFields(type);

            string[] names = new string[count];
            object[] values = new object[count];
            FieldTypeInfo field;
            for (int i = 0; i < count; ++i)
            {
                names[i] = ReadString();
                if (fields.TryGetValue(names[i], out field))
                {

                    values[i] = Unserialize(field.type, field.typeEnum);

                }
                else
                {
                    Unserialize();
                }
            }
            ObjectFieldModeUnserializer.Get(type, names).Unserialize(obj, values);
        }

        private void ReadMapAsObjectProperties(object obj, Type type, int count)
        {
            Dictionary<string, PropertyTypeInfo> properties = HproseHelper.GetProperties(type);

            string[] names = new string[count];
            object[] values = new object[count];
            PropertyTypeInfo property;
            for (int i = 0; i < count; ++i)
            {
                names[i] = ReadString();
                if (properties.TryGetValue(names[i], out property))
                {

                    values[i] = Unserialize(property.type, property.typeEnum);

                }
                else
                {
                    Unserialize();
                }
            }
            ObjectPropertyModeUnserializer.Get(type, names).Unserialize(obj, values);
        }

        private void ReadMapAsObjectMembers(object obj, Type type, int count)
        {
            Dictionary<string, MemberTypeInfo> members = HproseHelper.GetMembers(type);

            string[] names = new string[count];
            object[] values = new object[count];
            MemberTypeInfo member;
            for (int i = 0; i < count; ++i)
            {
                names[i] = ReadString();
                if (members.TryGetValue(names[i], out member))
                {


                    values[i] = Unserialize(member.type, member.typeEnum);
                }
                else
                {
                    Unserialize();
                }
            }
            ObjectMemberModeUnserializer.Get(type, names).Unserialize(obj, values);
        }

        private object ReadMapAsObject(Type type)
        {
            int count = ReadInt(HproseTags.TagOpenbrace);
            object obj = HproseHelper.NewInstance(type);
            if (obj == null) throw new HproseException("Can not make an instance of type: " + type.FullName);
            refer.Set(obj);
            if ((mode != HproseMode.MemberMode) && HproseHelper.IsSerializable(type))
            {
                if (mode == HproseMode.FieldMode)
                {
                    ReadMapAsObjectFields(obj, type, count);
                }
                else
                {
                    ReadMapAsObjectProperties(obj, type, count);
                }
            }
            else
            {
                ReadMapAsObjectMembers(obj, type, count);
            }

            stream.ReadByte();
            return obj;
        }

        private void ReadClass()
        {
            string className = ReadCharsAsString();
            int count = ReadInt(HproseTags.TagOpenbrace);
            string[] memberNames = new string[count];
            for (int i = 0; i < count; ++i)
            {
                memberNames[i] = ReadString();
            }
            stream.ReadByte();
            Type type = HproseHelper.GetClass(className);
            if (type == null)
            {
                object key = new object();
                classref.Add(key);
                membersref[key] = memberNames;
            }
            else
            {
                classref.Add(type);
                membersref[type] = memberNames;
            }
        }

        private object ReadRef()
        {
            return refer.Read(ReadIntWithoutTag());
        }

        private object ReadRef(Type type)
        {
            object obj = ReadRef();
            if (obj.GetType() == type) return obj;
            if (HproseHelper.IsAssignableFrom(type, obj.GetType())) return obj;
            throw CastError(obj, type);
        }

        public int ReadIntWithoutTag()
        {
            return ReadInt(HproseTags.TagSemicolon);
        }

        public BigInteger ReadBigIntegerWithoutTag()
        {
            return HproseHelper.ToBigInteger(ReadUntil(HproseTags.TagSemicolon).ToString());
        }

        public long ReadLongWithoutTag()
        {
            return ReadLong(HproseTags.TagSemicolon);
        }

        public double ReadDoubleWithoutTag()
        {
            return ParseDouble(ReadUntil(HproseTags.TagSemicolon));
        }

        public double ReadInfinityWithoutTag()
        {
            return ((stream.ReadByte() == HproseTags.TagNeg) ?
                double.NegativeInfinity : double.PositiveInfinity);
        }

        public DateTime ReadDateWithoutTag()
        {
            DateTime datetime;
            int year = stream.ReadByte() - '0';
            year = year * 10 + stream.ReadByte() - '0';
            year = year * 10 + stream.ReadByte() - '0';
            year = year * 10 + stream.ReadByte() - '0';
            int month = stream.ReadByte() - '0';
            month = month * 10 + stream.ReadByte() - '0';
            int day = stream.ReadByte() - '0';
            day = day * 10 + stream.ReadByte() - '0';
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagTime)
            {
                int hour = stream.ReadByte() - '0';
                hour = hour * 10 + stream.ReadByte() - '0';
                int minute = stream.ReadByte() - '0';
                minute = minute * 10 + stream.ReadByte() - '0';
                int second = stream.ReadByte() - '0';
                second = second * 10 + stream.ReadByte() - '0';
                int millisecond = 0;
                tag = stream.ReadByte();
                if (tag == HproseTags.TagPoint)
                {
                    millisecond = stream.ReadByte() - '0';
                    millisecond = millisecond * 10 + stream.ReadByte() - '0';
                    millisecond = millisecond * 10 + stream.ReadByte() - '0';
                    tag = stream.ReadByte();
                    if ((tag >= '0') && (tag <= '9'))
                    {
                        stream.ReadByte();
                        stream.ReadByte();
                        tag = stream.ReadByte();
                        if ((tag >= '0') && (tag <= '9'))
                        {
                            stream.ReadByte();
                            stream.ReadByte();
                            tag = stream.ReadByte();
                        }
                    }
                }
                DateTimeKind kind = (tag == HproseTags.TagUTC ? DateTimeKind.Utc : DateTimeKind.Local);
                datetime = new DateTime(year, month, day, hour, minute, second, millisecond, kind);

            }
            else
            {
                DateTimeKind kind = (tag == HproseTags.TagUTC ? DateTimeKind.Utc : DateTimeKind.Local);
                datetime = new DateTime(year, month, day, 0, 0, 0, kind);

            }
            refer.Set(datetime);
            return datetime;
        }

        public DateTime ReadTimeWithoutTag()
        {
            int hour = stream.ReadByte() - '0';
            hour = hour * 10 + stream.ReadByte() - '0';
            int minute = stream.ReadByte() - '0';
            minute = minute * 10 + stream.ReadByte() - '0';
            int second = stream.ReadByte() - '0';
            second = second * 10 + stream.ReadByte() - '0';
            int millisecond = 0;
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagPoint)
            {
                millisecond = stream.ReadByte() - '0';
                millisecond = millisecond * 10 + stream.ReadByte() - '0';
                millisecond = millisecond * 10 + stream.ReadByte() - '0';
                tag = stream.ReadByte();
                if ((tag >= '0') && (tag <= '9'))
                {
                    stream.ReadByte();
                    stream.ReadByte();
                    tag = stream.ReadByte();
                    if ((tag >= '0') && (tag <= '9'))
                    {
                        stream.ReadByte();
                        stream.ReadByte();
                        tag = stream.ReadByte();
                    }
                }
            }
            DateTimeKind kind = (tag == HproseTags.TagUTC ? DateTimeKind.Utc : DateTimeKind.Local);
            DateTime datetime = new DateTime(1970, 1, 1, hour, minute, second, millisecond, kind);

            refer.Set(datetime);
            return datetime;
        }

        public byte[] ReadBytesWithoutTag()
        {
            int len = ReadInt(HproseTags.TagQuote);
            int off = 0;
            byte[] b = new byte[len];
            while (len > 0)
            {
                int size = stream.Read(b, off, len);
                off += size;
                len -= size;
            }
            stream.ReadByte();
            refer.Set(b);
            return b;
        }

        public string ReadUTF8CharWithoutTag()
        {
            return new string(ReadUTF8CharAsChar(), 1);
        }

        public String ReadStringWithoutTag()
        {
            String str = ReadCharsAsString();
            refer.Set(str);
            return str;
        }

        public char[] ReadCharsWithoutTag()
        {
            char[] chars = ReadChars();
            refer.Set(chars);
            return chars;
        }

        public Guid ReadGuidWithoutTag()
        {
            byte[] buf = new byte[38];
            stream.Read(buf, 0, 38);
            Guid guid = HproseHelper.ToGuid(buf);
            refer.Set(guid);
            return guid;
        }

        public IList ReadListWithoutTag()
        {
            int count = ReadInt(HproseTags.TagOpenbrace);

            List<object> a = new List<object>(count);
            refer.Set(a);
            for (int i = 0; i < count; ++i)
            {
                a.Add(Unserialize());
            }
            stream.ReadByte();
            return a;
        }

        public IDictionary ReadMapWithoutTag()
        {
            int count = ReadInt(HproseTags.TagOpenbrace);

            HashMap<object, object> map = new HashMap<object, object>(count);
            refer.Set(map);
            for (int i = 0; i < count; ++i)
            {
                object key = Unserialize();
                object value = Unserialize();
                map[key] = value;
            }
            stream.ReadByte();
            return map;
        }

        private void ReadObjectFields(object obj, Type type, int count, string[] memberNames)
        {
            object[] values = new object[count];
            Dictionary<string, FieldTypeInfo> fields = HproseHelper.GetFields(type);

            FieldTypeInfo field;
            for (int i = 0; i < count; ++i)
            {
                if (fields.TryGetValue(memberNames[i], out field))
                {

                    values[i] = Unserialize(field.type, field.typeEnum);

                }
                else
                {
                    Unserialize();
                }
            }
            ObjectFieldModeUnserializer.Get(type, memberNames).Unserialize(obj, values);
        }

        private void ReadObjectProperties(object obj, Type type, int count, string[] memberNames)
        {
            object[] values = new object[count];
            Dictionary<string, PropertyTypeInfo> properties = HproseHelper.GetProperties(type);

            PropertyTypeInfo property;
            for (int i = 0; i < count; ++i)
            {
                if (properties.TryGetValue(memberNames[i], out property))
                {

                    values[i] = Unserialize(property.type, property.typeEnum);

                }
                else
                {
                    Unserialize();
                }
            }
            ObjectPropertyModeUnserializer.Get(type, memberNames).Unserialize(obj, values);
        }

        private void ReadObjectMembers(object obj, Type type, int count, string[] memberNames)
        {
            object[] values = new object[count];
            Dictionary<string, MemberTypeInfo> members = HproseHelper.GetMembers(type);

            MemberTypeInfo member;
            for (int i = 0; i < count; ++i)
            {
                if (members.TryGetValue(memberNames[i], out member))
                {
                    values[i] = Unserialize(member.type, member.typeEnum);
                }
                else
                {
                    Unserialize();
                }
            }
            ObjectMemberModeUnserializer.Get(type, memberNames).Unserialize(obj, values);
        }

        public object ReadObjectWithoutTag(Type type)
        {
            object c = classref[ReadInt(HproseTags.TagOpenbrace)];
            string[] memberNames = membersref[c];

            int count = memberNames.Length;
            object obj = null;
            if (c is Type)
            {
                Type cls = (Type)c;
                if ((type == null) || HproseHelper.IsAssignableFrom(type, cls)) type = cls;
            }
            if (type == null)
            {

                Dictionary<string, object> map = new Dictionary<string, object>(count);
                refer.Set(map);
                for (int i = 0; i < count; ++i)
                {
                    map[memberNames[i]] = Unserialize();
                }
                obj = map;
            }
            else
            {
                obj = HproseHelper.NewInstance(type);
                if (obj == null) throw new HproseException("Can not make an instance of type: " + type.FullName);
                refer.Set(obj);
                if ((mode != HproseMode.MemberMode) && HproseHelper.IsSerializable(type))
                {
                    if (mode == HproseMode.FieldMode)
                    {
                        ReadObjectFields(obj, type, count, memberNames);
                    }
                    else
                    {
                        ReadObjectProperties(obj, type, count, memberNames);
                    }
                }
                else
                {
                    ReadObjectMembers(obj, type, count, memberNames);
                }

            }
            stream.ReadByte();
            return obj;
        }

        public T Unserialize<T>()
        {
            return (T)Unserialize(typeof(T), HproseHelper.GetTypeEnum(typeof(T)));
        }

        private object Unserialize(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return ReadIntWithoutTag();
                case HproseTags.TagLong: return ReadBigIntegerWithoutTag();
                case HproseTags.TagDouble: return ReadDoubleWithoutTag();
                case HproseTags.TagNull: return null;
                case HproseTags.TagEmpty: return "";
                case HproseTags.TagTrue: return true;
                case HproseTags.TagFalse: return false;
                case HproseTags.TagNaN: return double.NaN;
                case HproseTags.TagInfinity: return ReadInfinityWithoutTag();
                case HproseTags.TagDate: return ReadDateWithoutTag();
                case HproseTags.TagTime: return ReadTimeWithoutTag();
                case HproseTags.TagBytes: return ReadBytesWithoutTag();
                case HproseTags.TagUTF8Char: return ReadUTF8CharWithoutTag();
                case HproseTags.TagString: return ReadStringWithoutTag();
                case HproseTags.TagGuid: return ReadGuidWithoutTag();
                case HproseTags.TagList: return ReadListWithoutTag();
                case HproseTags.TagMap: return ReadMapWithoutTag();
                case HproseTags.TagClass: ReadClass(); return ReadObject(null);
                case HproseTags.TagObject: return ReadObjectWithoutTag(null);
                case HproseTags.TagRef: return ReadRef();
                case HproseTags.TagError: throw new HproseException(ReadString());
                default: throw UnexpectedTag(tag);
            }
        }

        public object Unserialize()
        {
            return Unserialize(stream.ReadByte());
        }

        public object ReadObject()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagUTF8Char) return ReadUTF8CharAsChar();
            return Unserialize(tag);
        }

        private string TagToString(int tag)
        {
            switch (tag)
            {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case HproseTags.TagInteger: return "Integer";
                case HproseTags.TagLong: return "BigInteger";
                case HproseTags.TagDouble: return "Double";
                case HproseTags.TagNull: return "Null";
                case HproseTags.TagEmpty: return "Empty String";
                case HproseTags.TagTrue: return "Boolean True";
                case HproseTags.TagFalse: return "Boolean False";
                case HproseTags.TagNaN: return "NaN";
                case HproseTags.TagInfinity: return "Infinity";
                case HproseTags.TagDate: return "DateTime";
                case HproseTags.TagTime: return "DateTime";
                case HproseTags.TagBytes: return "Byte[]";
                case HproseTags.TagUTF8Char: return "Char";
                case HproseTags.TagString: return "String";
                case HproseTags.TagGuid: return "Guid";
                case HproseTags.TagList: return "IList";
                case HproseTags.TagMap: return "IDictionary";
                case HproseTags.TagClass: return "Class";
                case HproseTags.TagObject: return "Object";
                case HproseTags.TagRef: return "Object Reference";
                case HproseTags.TagError: throw new HproseException(ReadString());
                default: throw UnexpectedTag(tag);
            }
        }
        public DBNull ReadDBNull()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case '0':
                case HproseTags.TagNull:
                case HproseTags.TagEmpty:
                case HproseTags.TagFalse: return DBNull.Value;
                default: throw CastError(TagToString(tag), HproseHelper.typeofDBNull);
            }
        }

        private bool ReadBooleanWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return false;
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': return true;
                case HproseTags.TagInteger: return ReadIntWithoutTag() != 0;
                case HproseTags.TagLong: return !(ReadBigIntegerWithoutTag().IsZero);
                case HproseTags.TagDouble: return ReadDoubleWithoutTag() != 0.0;
                case HproseTags.TagEmpty: return false;
                case HproseTags.TagTrue: return true;
                case HproseTags.TagFalse: return false;
                case HproseTags.TagNaN: return true;
                case HproseTags.TagInfinity: stream.ReadByte(); return true;
                case HproseTags.TagUTF8Char: char c = ReadUTF8CharAsChar(); return (c != '\0') && (c != '0');
                case HproseTags.TagString: return HproseHelper.ToBoolean(ReadStringWithoutTag());
                case HproseTags.TagRef: return HproseHelper.ToBoolean(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofBoolean);
            }
        }

        public bool ReadBoolean()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? false : ReadBooleanWithTag(tag);
        }

        public bool? ReadNullableBoolean()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadBooleanWithTag(tag);
            }
        }

        private char ReadCharWithTag(int tag)
        {
            switch (tag)
            {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': return (char)tag;
                case HproseTags.TagInteger: return (char)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (char)(ReadLongWithoutTag());
                case HproseTags.TagUTF8Char: return ReadUTF8CharAsChar();
                case HproseTags.TagString: return (ReadStringWithoutTag())[0];
                case HproseTags.TagRef: return (ReadRef().ToString())[0];
                default: throw CastError(TagToString(tag), HproseHelper.typeofChar);
            }
        }

        public char ReadChar()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? (char)0 : ReadCharWithTag(tag);
        }

        public char? ReadNullableChar()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadCharWithTag(tag);
            }
        }

        private sbyte ReadSByteWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return (sbyte)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (sbyte)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (sbyte)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToSByte(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToSByte(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToSByte(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofSByte);
            }
        }


        public sbyte ReadSByte()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? (sbyte)0 : ReadSByteWithTag(tag);
        }


        public sbyte? ReadNullableSByte()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadSByteWithTag(tag);
            }
        }

        private byte ReadByteWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return (byte)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (byte)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (byte)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToByte(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToByte(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToByte(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofByte);
            }
        }

        public byte ReadByte()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? (byte)0 : ReadByteWithTag(tag);
        }

        public byte? ReadNullableByte()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadByteWithTag(tag);
            }
        }

        private short ReadInt16WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return (short)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (short)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (short)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToInt16(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToInt16(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToInt16(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt16);
            }
        }

        public short ReadInt16()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? (short)0 : ReadInt16WithTag(tag);
        }

        public short? ReadNullableInt16()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadInt16WithTag(tag);
            }
        }

        private ushort ReadUInt16WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return (ushort)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (ushort)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (ushort)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToUInt16(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToUInt16(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToUInt16(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt16);
            }
        }


        public ushort ReadUInt16()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? (ushort)0 : ReadUInt16WithTag(tag);
        }


        public ushort? ReadNullableUInt16()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadUInt16WithTag(tag);
            }
        }

        private int ReadInt32WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return ReadIntWithoutTag();
                case HproseTags.TagLong: return (int)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (int)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToInt32(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToInt32(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToInt32(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt32);
            }
        }

        public int ReadInt32()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0 : ReadInt32WithTag(tag);
        }

        public int? ReadNullableInt32()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadInt32WithTag(tag);
            }
        }

        private uint ReadUInt32WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0;
                case '1': return 1;
                case '2': return 2;
                case '3': return 3;
                case '4': return 4;
                case '5': return 5;
                case '6': return 6;
                case '7': return 7;
                case '8': return 8;
                case '9': return 9;
                case HproseTags.TagInteger: return (uint)(ReadIntWithoutTag());
                case HproseTags.TagLong: return (uint)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (uint)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0;
                case HproseTags.TagTrue: return 1;
                case HproseTags.TagFalse: return 0;
                case HproseTags.TagUTF8Char: return Convert.ToUInt32(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToUInt32(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToUInt32(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt32);
            }
        }


        public uint ReadUInt32()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0 : ReadUInt32WithTag(tag);
        }


        public uint? ReadNullableUInt32()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadUInt32WithTag(tag);
            }
        }

        private long ReadInt64WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0L;
                case '1': return 1L;
                case '2': return 2L;
                case '3': return 3L;
                case '4': return 4L;
                case '5': return 5L;
                case '6': return 6L;
                case '7': return 7L;
                case '8': return 8L;
                case '9': return 9L;
                case HproseTags.TagInteger: return ReadLongWithoutTag();
                case HproseTags.TagLong: return ReadLongWithoutTag();
                case HproseTags.TagDouble: return (long)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0L;
                case HproseTags.TagTrue: return 1L;
                case HproseTags.TagFalse: return 0L;
                case HproseTags.TagDate: return ReadDateWithoutTag().Ticks;
                case HproseTags.TagTime: return ReadTimeWithoutTag().Ticks;
                case HproseTags.TagUTF8Char: return Convert.ToInt64(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToInt64(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToInt64(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt64);
            }
        }

        public long ReadInt64()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0L : ReadInt64WithTag(tag);
        }

        public long? ReadNullableInt64()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadInt64WithTag(tag);
            }
        }

        private ulong ReadUInt64WithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0L;
                case '1': return 1L;
                case '2': return 2L;
                case '3': return 3L;
                case '4': return 4L;
                case '5': return 5L;
                case '6': return 6L;
                case '7': return 7L;
                case '8': return 8L;
                case '9': return 9L;
                case HproseTags.TagInteger: return (ulong)(ReadLongWithoutTag());
                case HproseTags.TagLong: return (ulong)(ReadLongWithoutTag());
                case HproseTags.TagDouble: return (ulong)(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return 0L;
                case HproseTags.TagTrue: return 1L;
                case HproseTags.TagFalse: return 0L;
                case HproseTags.TagUTF8Char: return Convert.ToUInt64(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToUInt64(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToUInt64(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt64);
            }
        }


        public ulong ReadUInt64()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0L : ReadUInt64WithTag(tag);
        }


        public ulong? ReadNullableUInt64()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadUInt64WithTag(tag);
            }
        }

        private IntPtr ReadIntPtrWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return IntPtr.Zero;
                case '1': return new IntPtr(1);
                case '2': return new IntPtr(2);
                case '3': return new IntPtr(3);
                case '4': return new IntPtr(4);
                case '5': return new IntPtr(5);
                case '6': return new IntPtr(6);
                case '7': return new IntPtr(7);
                case '8': return new IntPtr(8);
                case '9': return new IntPtr(9);
                case HproseTags.TagInteger: return new IntPtr(ReadIntWithoutTag());
                case HproseTags.TagLong: return new IntPtr(ReadLongWithoutTag());
                case HproseTags.TagDouble: return new IntPtr((long)ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return IntPtr.Zero;
                case HproseTags.TagTrue: return new IntPtr(1);
                case HproseTags.TagFalse: return IntPtr.Zero;
                case HproseTags.TagUTF8Char: return new IntPtr(Convert.ToInt64(ReadUTF8CharWithoutTag()));
                case HproseTags.TagString: return new IntPtr(Convert.ToInt64(ReadStringWithoutTag()));
                case HproseTags.TagRef: return new IntPtr(Convert.ToInt64(ReadRef().ToString()));
                default: throw CastError(TagToString(tag), HproseHelper.typeofIntPtr);
            }
        }

        public IntPtr ReadIntPtr()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? IntPtr.Zero : ReadIntPtrWithTag(tag);
        }


        public IntPtr? ReadNullableIntPtr()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadIntPtrWithTag(tag);
            }
        }

        private float ReadSingleWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0.0F;
                case '1': return 1.0F;
                case '2': return 2.0F;
                case '3': return 3.0F;
                case '4': return 4.0F;
                case '5': return 5.0F;
                case '6': return 6.0F;
                case '7': return 7.0F;
                case '8': return 8.0F;
                case '9': return 9.0F;
                case HproseTags.TagInteger: return ReadIntAsFloat();
                case HproseTags.TagLong: return ReadIntAsFloat();
                case HproseTags.TagDouble: return ParseFloat(ReadUntil(HproseTags.TagSemicolon));
                case HproseTags.TagEmpty: return 0.0F;
                case HproseTags.TagTrue: return 1.0F;
                case HproseTags.TagFalse: return 0.0F;
                case HproseTags.TagNaN: return float.NaN;
                case HproseTags.TagInfinity:
                    return (stream.ReadByte() == HproseTags.TagPos) ?
                            float.PositiveInfinity :
                            float.NegativeInfinity;
                case HproseTags.TagUTF8Char: return Convert.ToSingle(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToSingle(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToSingle(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofSingle);
            }
        }

        public float ReadSingle()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0.0F : ReadSingleWithTag(tag);
        }

        public float? ReadNullableSingle()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadSingleWithTag(tag);
            }
        }

        private double ReadDoubleWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0.0;
                case '1': return 1.0;
                case '2': return 2.0;
                case '3': return 3.0;
                case '4': return 4.0;
                case '5': return 5.0;
                case '6': return 6.0;
                case '7': return 7.0;
                case '8': return 8.0;
                case '9': return 9.0;
                case HproseTags.TagInteger: return ReadIntAsDouble();
                case HproseTags.TagLong: return ReadIntAsDouble();
                case HproseTags.TagDouble: return ReadDoubleWithoutTag();
                case HproseTags.TagEmpty: return 0.0;
                case HproseTags.TagTrue: return 1.0;
                case HproseTags.TagFalse: return 0.0;
                case HproseTags.TagNaN: return double.NaN;
                case HproseTags.TagInfinity: return ReadInfinityWithoutTag();
                case HproseTags.TagUTF8Char: return Convert.ToDouble(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToDouble(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToDouble(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofDouble);
            }
        }

        public double ReadDouble()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0.0 : ReadDoubleWithTag(tag);
        }

        public double? ReadNullableDouble()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadDoubleWithTag(tag);
            }
        }

        private decimal ReadDecimalWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return 0.0M;
                case '1': return 1.0M;
                case '2': return 2.0M;
                case '3': return 3.0M;
                case '4': return 4.0M;
                case '5': return 5.0M;
                case '6': return 6.0M;
                case '7': return 7.0M;
                case '8': return 8.0M;
                case '9': return 9.0M;
                case HproseTags.TagInteger: return ReadIntAsDecimal();
                case HproseTags.TagLong: return ReadIntAsDecimal();
                case HproseTags.TagDouble: return decimal.Parse(ReadUntil(HproseTags.TagSemicolon).ToString());
                case HproseTags.TagEmpty: return 0.0M;
                case HproseTags.TagTrue: return 1.0M;
                case HproseTags.TagFalse: return 0.0M;
                case HproseTags.TagUTF8Char: return Convert.ToDecimal(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return Convert.ToDecimal(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToDecimal(ReadRef());
                default: throw CastError(TagToString(tag), HproseHelper.typeofDecimal);
            }
        }

        public decimal ReadDecimal()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? 0.0M : ReadDecimalWithTag(tag);
        }

        public decimal? ReadNullableDecimal()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadDecimalWithTag(tag);
            }
        }

        private DateTime ReadDateTimeWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return new DateTime(0L);
                case '1': return new DateTime(1L);
                case '2': return new DateTime(2L);
                case '3': return new DateTime(3L);
                case '4': return new DateTime(4L);
                case '5': return new DateTime(5L);
                case '6': return new DateTime(6L);
                case '7': return new DateTime(7L);
                case '8': return new DateTime(8L);
                case '9': return new DateTime(9L);
                case HproseTags.TagInteger: return new DateTime(ReadLongWithoutTag());
                case HproseTags.TagLong: return new DateTime(ReadLongWithoutTag());
                case HproseTags.TagDouble: return new DateTime((long)ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return new DateTime(0L);
                case HproseTags.TagDate: return ReadDateWithoutTag();
                case HproseTags.TagTime: return ReadTimeWithoutTag();
                case HproseTags.TagString: return Convert.ToDateTime(ReadStringWithoutTag());
                case HproseTags.TagRef: return Convert.ToDateTime(ReadRef());

                default: throw CastError(TagToString(tag), HproseHelper.typeofDateTime);
            }
        }

        public DateTime ReadDateTime()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? DateTime.MinValue : ReadDateTimeWithTag(tag);
        }

        public DateTime? ReadNullableDateTime()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadDateTimeWithTag(tag);
            }
        }

        public string ReadString()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case '0': return "0";
                case '1': return "1";
                case '2': return "2";
                case '3': return "3";
                case '4': return "4";
                case '5': return "5";
                case '6': return "6";
                case '7': return "7";
                case '8': return "8";
                case '9': return "9";
                case HproseTags.TagInteger: return ReadUntil(HproseTags.TagSemicolon).ToString();
                case HproseTags.TagLong: return ReadUntil(HproseTags.TagSemicolon).ToString();
                case HproseTags.TagDouble: return ReadUntil(HproseTags.TagSemicolon).ToString();
                case HproseTags.TagNull: return null;
                case HproseTags.TagEmpty: return "";
                case HproseTags.TagTrue: return bool.TrueString;
                case HproseTags.TagFalse: return bool.FalseString;
                case HproseTags.TagNaN: return double.NaN.ToString();
                case HproseTags.TagInfinity: return ReadInfinityWithoutTag().ToString();
                case HproseTags.TagDate: return ReadDateWithoutTag().ToString();
                case HproseTags.TagTime: return ReadTimeWithoutTag().ToString();
                case HproseTags.TagUTF8Char: return ReadUTF8CharWithoutTag();
                case HproseTags.TagString: return ReadStringWithoutTag();
                case HproseTags.TagGuid: return ReadGuidWithoutTag().ToString();
                case HproseTags.TagList: return ReadListWithoutTag().ToString();
                case HproseTags.TagMap: return ReadMapWithoutTag().ToString();
                case HproseTags.TagClass: ReadClass(); return ReadObject(null).ToString();
                case HproseTags.TagObject: return ReadObjectWithoutTag(null).ToString();
                case HproseTags.TagRef:
                    {
                        object obj = ReadRef();
                        if (obj is char[]) return new String((char[])obj);
                        return obj.ToString();
                    }
                default: throw CastError(TagToString(tag), HproseHelper.typeofString);
            }
        }

        public StringBuilder ReadStringBuilder()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case '0': return new StringBuilder("0");
                case '1': return new StringBuilder("1");
                case '2': return new StringBuilder("2");
                case '3': return new StringBuilder("3");
                case '4': return new StringBuilder("4");
                case '5': return new StringBuilder("5");
                case '6': return new StringBuilder("6");
                case '7': return new StringBuilder("7");
                case '8': return new StringBuilder("8");
                case '9': return new StringBuilder("9");
                case HproseTags.TagInteger: return ReadUntil(HproseTags.TagSemicolon);
                case HproseTags.TagLong: return ReadUntil(HproseTags.TagSemicolon);
                case HproseTags.TagDouble: return ReadUntil(HproseTags.TagSemicolon);
                case HproseTags.TagNull: return null;
                case HproseTags.TagEmpty: return new StringBuilder();
                case HproseTags.TagTrue: return new StringBuilder(bool.TrueString);
                case HproseTags.TagFalse: return new StringBuilder(bool.FalseString);
                case HproseTags.TagNaN: return new StringBuilder(double.NaN.ToString());
                case HproseTags.TagInfinity: return new StringBuilder(ReadInfinityWithoutTag().ToString());
                case HproseTags.TagDate: return new StringBuilder(ReadDateWithoutTag().ToString());
                case HproseTags.TagTime: return new StringBuilder(ReadTimeWithoutTag().ToString());
                case HproseTags.TagUTF8Char: return new StringBuilder(1).Append(ReadUTF8CharAsChar());
                case HproseTags.TagString: return new StringBuilder(ReadStringWithoutTag());
                case HproseTags.TagGuid: return new StringBuilder(ReadGuidWithoutTag().ToString());
                case HproseTags.TagRef:
                    {
                        object obj = ReadRef();
                        if (obj is char[]) return new StringBuilder(new String((char[])obj));
                        if (obj is StringBuilder) return (StringBuilder)obj;
                        return new StringBuilder(obj.ToString());
                    }
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringBuilder);
            }
        }

        private Guid ReadGuidWithTag(int tag)
        {
            switch (tag)
            {
                case HproseTags.TagBytes: return new Guid(ReadBytesWithoutTag());
                case HproseTags.TagGuid: return ReadGuidWithoutTag();
                case HproseTags.TagString: return HproseHelper.ToGuid(ReadStringWithoutTag());
                case HproseTags.TagRef:
                    {
                        object obj = ReadRef();
                        if (obj is Guid) return (Guid)obj;
                        if (obj is byte[]) return new Guid((byte[])obj);
                        if (obj is string) return HproseHelper.ToGuid((string)obj);
                        if (obj is char[]) return HproseHelper.ToGuid((char[])obj);
                        throw CastError(obj, HproseHelper.typeofGuid);
                    }
                default: throw CastError(TagToString(tag), HproseHelper.typeofGuid);
            }
        }

        public Guid ReadGuid()
        {
            return ReadGuidWithTag(stream.ReadByte());
        }

        public Guid? ReadNullableGuid()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadGuidWithTag(tag);
            }
        }

        private BigInteger ReadBigIntegerWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return BigInteger.Zero;
                case '1': return BigInteger.One;
                case '2': return new BigInteger(2);
                case '3': return new BigInteger(3);
                case '4': return new BigInteger(4);
                case '5': return new BigInteger(5);
                case '6': return new BigInteger(6);
                case '7': return new BigInteger(7);
                case '8': return new BigInteger(8);
                case '9': return new BigInteger(9);
                case HproseTags.TagInteger: return new BigInteger(ReadIntWithoutTag());
                case HproseTags.TagLong: return ReadBigIntegerWithoutTag();
                case HproseTags.TagDouble: return new BigInteger(ReadDoubleWithoutTag());
                case HproseTags.TagEmpty: return BigInteger.Zero;
                case HproseTags.TagTrue: return BigInteger.One;
                case HproseTags.TagFalse: return BigInteger.Zero;
                case HproseTags.TagDate: return new BigInteger(ReadDateWithoutTag().Ticks);
                case HproseTags.TagTime: return new BigInteger(ReadTimeWithoutTag().Ticks);
                case HproseTags.TagUTF8Char: return HproseHelper.ToBigInteger(ReadUTF8CharWithoutTag());
                case HproseTags.TagString: return HproseHelper.ToBigInteger(ReadStringWithoutTag());
                case HproseTags.TagRef: return HproseHelper.ToBigInteger(ReadRef().ToString());
                default: throw CastError(TagToString(tag), HproseHelper.typeofBigInteger);
            }
        }

        public BigInteger ReadBigInteger()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? BigInteger.Zero : ReadBigIntegerWithTag(tag);
        }

        public BigInteger? ReadNullableBigInteger()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadBigIntegerWithTag(tag);
            }
        }

        private TimeSpan ReadTimeSpanWithTag(int tag)
        {
            switch (tag)
            {
                case '0': return TimeSpan.Zero;
                case '1': return new TimeSpan(1L);
                case '2': return new TimeSpan(2L);
                case '3': return new TimeSpan(3L);
                case '4': return new TimeSpan(4L);
                case '5': return new TimeSpan(5L);
                case '6': return new TimeSpan(6L);
                case '7': return new TimeSpan(7L);
                case '8': return new TimeSpan(8L);
                case '9': return new TimeSpan(9L);
                case HproseTags.TagInteger: return new TimeSpan(ReadLongWithoutTag());
                case HproseTags.TagLong: return new TimeSpan(ReadLongWithoutTag());
                case HproseTags.TagDouble: return new TimeSpan((long)(ReadDoubleWithoutTag()));
                case HproseTags.TagEmpty: return TimeSpan.Zero;
                case HproseTags.TagTrue: return new TimeSpan(1L);
                case HproseTags.TagFalse: return TimeSpan.Zero;
                case HproseTags.TagDate: return new TimeSpan(ReadDateWithoutTag().Ticks);
                case HproseTags.TagTime: return new TimeSpan(ReadTimeWithoutTag().Ticks);
                case HproseTags.TagString: return new TimeSpan(Convert.ToDateTime(ReadStringWithoutTag()).Ticks);
                case HproseTags.TagRef: return new TimeSpan(Convert.ToDateTime(ReadRef()).Ticks);

                default: throw CastError(TagToString(tag), HproseHelper.typeofTimeSpan);
            }
        }

        public TimeSpan ReadTimeSpan()
        {
            int tag = stream.ReadByte();
            return (tag == HproseTags.TagNull) ? TimeSpan.Zero : ReadTimeSpanWithTag(tag);
        }

        public TimeSpan? ReadNullableTimeSpan()
        {
            int tag = stream.ReadByte();
            if (tag == HproseTags.TagNull)
            {
                return null;
            }
            else
            {
                return ReadTimeSpanWithTag(tag);
            }
        }

        public MemoryStream ReadStream()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagUTF8Char: return ReadUTF8CharAsStream();
                case HproseTags.TagString: return ReadCharsAsStream();
                case HproseTags.TagBytes: return ReadBytesAsStream();
                case HproseTags.TagRef: return (MemoryStream)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStream);
            }
        }

        public object ReadEnum(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case '0': return Enum.ToObject(type, 0);
                case '1': return Enum.ToObject(type, 1);
                case '2': return Enum.ToObject(type, 2);
                case '3': return Enum.ToObject(type, 3);
                case '4': return Enum.ToObject(type, 4);
                case '5': return Enum.ToObject(type, 5);
                case '6': return Enum.ToObject(type, 6);
                case '7': return Enum.ToObject(type, 7);
                case '8': return Enum.ToObject(type, 8);
                case '9': return Enum.ToObject(type, 9);
                case HproseTags.TagInteger: return Enum.ToObject(type, ReadIntWithoutTag());
                case HproseTags.TagLong: return Enum.ToObject(type, ReadLongWithoutTag());
                case HproseTags.TagDouble: return Enum.ToObject(type, Convert.ToInt64(ReadDoubleWithoutTag()));
                case HproseTags.TagNull: return Enum.ToObject(type, 0);
                case HproseTags.TagEmpty: return Enum.ToObject(type, 0);
                case HproseTags.TagTrue: return Enum.ToObject(type, 1);
                case HproseTags.TagFalse: return Enum.ToObject(type, 0);
                case HproseTags.TagUTF8Char: return Enum.Parse(type, ReadUTF8CharWithoutTag(), true);
                case HproseTags.TagString: return Enum.Parse(type, ReadStringWithoutTag(), true);
                case HproseTags.TagRef: return Enum.Parse(type, ReadRef().ToString(), true);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public void ReadArray(Type[] types, object[] a, int count)
        {
            refer.Set(a);
            for (int i = 0; i < count; ++i)
            {
                a[i] = Unserialize(types[i]);
            }
            stream.ReadByte();
        }

        public object[] ReadArray(int count)
        {
            object[] a = new object[count];
            refer.Set(a);
            for (int i = 0; i < count; ++i)
            {
                a[i] = Unserialize();
            }
            stream.ReadByte();
            return a;
        }

        public object[] ReadObjectArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadArray(ReadInt(HproseTags.TagOpenbrace));
                case HproseTags.TagRef: return (object[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofObjectArray);
            }
        }

        public bool[] ReadBooleanArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        bool[] a = new bool[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadBoolean();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (bool[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBooleanArray);
            }
        }

        public char[] ReadCharArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagUTF8Char: return new char[] { ReadUTF8CharAsChar() };
                case HproseTags.TagString: return ReadCharsWithoutTag();
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        char[] a = new char[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadChar();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef:
                    {
                        object obj = ReadRef();
                        if (obj is char[]) return (char[])obj;
                        if (obj is string) return ((string)obj).ToCharArray();
                        if (obj is List<char>) return ((List<char>)obj).ToArray();
                        throw CastError(obj, HproseHelper.typeofCharArray);
                    }
                default: throw CastError(TagToString(tag), HproseHelper.typeofCharArray);
            }
        }


        public sbyte[] ReadSByteArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        sbyte[] a = new sbyte[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadSByte();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (sbyte[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofSByteArray);
            }
        }

        public byte[] ReadByteArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagEmpty: return new byte[0];
                case HproseTags.TagUTF8Char: return ReadUTF8CharAsStream().ToArray();
                case HproseTags.TagString: return ReadCharsAsStream().ToArray();
                case HproseTags.TagGuid: return ReadGuidWithoutTag().ToByteArray();
                case HproseTags.TagBytes: return ReadBytesWithoutTag();
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        byte[] a = new byte[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadByte();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef:
                    {
                        object obj = ReadRef();
                        if (obj is byte[]) return (byte[])obj;
                        if (obj is Guid) return ((Guid)obj).ToByteArray();
                        if (obj is MemoryStream) return ((MemoryStream)obj).ToArray();
                        if (obj is List<byte>) return ((List<byte>)obj).ToArray();
                        throw CastError(obj, HproseHelper.typeofByteArray);
                    }
                default: throw CastError(TagToString(tag), HproseHelper.typeofByteArray);
            }
        }

        public short[] ReadInt16Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        short[] a = new short[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadInt16();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (short[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt16Array);
            }
        }


        public ushort[] ReadUInt16Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        ushort[] a = new ushort[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadUInt16();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (ushort[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt16Array);
            }
        }

        public int[] ReadInt32Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        int[] a = new int[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadInt32();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (int[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt32Array);
            }
        }


        public uint[] ReadUInt32Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        uint[] a = new uint[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadUInt32();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (uint[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt32Array);
            }
        }

        public long[] ReadInt64Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        long[] a = new long[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadInt64();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (long[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt64Array);
            }
        }


        public ulong[] ReadUInt64Array()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        ulong[] a = new ulong[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadUInt64();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (ulong[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt64Array);
            }
        }

        public IntPtr[] ReadIntPtrArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        IntPtr[] a = new IntPtr[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadIntPtr();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (IntPtr[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofIntPtrArray);
            }
        }

        public float[] ReadSingleArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        float[] a = new float[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadSingle();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (float[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofSingleArray);
            }
        }

        public double[] ReadDoubleArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        double[] a = new double[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadDouble();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (double[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDoubleArray);
            }
        }

        public decimal[] ReadDecimalArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        decimal[] a = new decimal[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadDecimal();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (decimal[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDecimalArray);
            }
        }

        public DateTime[] ReadDateTimeArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        DateTime[] a = new DateTime[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadDateTime();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (DateTime[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDateTimeArray);
            }
        }

        public string[] ReadStringArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        string[] a = new string[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadString();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (string[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringArray);
            }
        }

        public StringBuilder[] ReadStringBuilderArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        StringBuilder[] a = new StringBuilder[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadStringBuilder();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (StringBuilder[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringBuilderArray);
            }
        }

        public Guid[] ReadGuidArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Guid[] a = new Guid[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadGuid();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (Guid[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofGuidArray);
            }
        }

        public BigInteger[] ReadBigIntegerArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        BigInteger[] a = new BigInteger[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadBigInteger();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (BigInteger[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBigIntegerArray);
            }
        }

        public TimeSpan[] ReadTimeSpanArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        TimeSpan[] a = new TimeSpan[count];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadTimeSpan();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (TimeSpan[])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofTimeSpanArray);
            }
        }


        public char[][] ReadCharsArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        char[][] a = new char[count][];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadCharArray();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (char[][])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofCharsArray);
            }
        }


        public byte[][] ReadBytesArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        byte[][] a = new byte[count][];
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadByteArray();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (byte[][])ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBytesArray);
            }
        }

        private Array ReadArray(Type type)
        {
            int count = ReadInt(HproseTags.TagOpenbrace);
            int rank = type.GetArrayRank();
            Type elementType = type.GetElementType();
            TypeEnum elementTypeEnum = HproseHelper.GetTypeEnum(elementType);
            Array a;
            if (rank == 1)
            {
                a = Array.CreateInstance(elementType, count);
                refer.Set(a);
                for (int i = 0; i < count; ++i)
                {
                    a.SetValue(Unserialize(elementType, elementTypeEnum), i);

                }
            }
            else
            {
                int i;
                int[] loc = new int[rank];
                int[] len = new int[rank];
                int maxrank = rank - 1;
                len[0] = count;
                for (i = 1; i < rank; ++i)
                {
                    stream.ReadByte();
                    //CheckTag(HproseTags.TagList);
                    len[i] = ReadInt(HproseTags.TagOpenbrace);
                }
                a = Array.CreateInstance(elementType, len);
                refer.Set(a);
                for (i = 1; i < rank; ++i)
                {
                    refer.Set(null);
                }
                while (true)
                {
                    for (loc[maxrank] = 0;
                         loc[maxrank] < len[maxrank];
                         loc[maxrank]++)
                    {
                        a.SetValue(Unserialize(elementType, elementTypeEnum), loc);
                    }
                    for (i = maxrank; i > 0; i--)
                    {
                        if (loc[i] >= len[i])
                        {
                            loc[i] = 0;
                            loc[i - 1]++;
                            stream.ReadByte();
                            //CheckTag(HproseTags.TagClosebrace);
                        }
                    }
                    if (loc[0] >= len[0])
                    {
                        break;
                    }
                    int n = 0;
                    for (i = maxrank; i > 0; i--)
                    {
                        if (loc[i] == 0)
                        {
                            n++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    for (i = rank - n; i < rank; ++i)
                    {
                        stream.ReadByte();
                        //CheckTag(HproseTags.TagList);
                        refer.Set(null);
                        SkipUntil(HproseTags.TagOpenbrace);
                    }
                }
            }
            stream.ReadByte();
            return a;
        }

        public Array ReadOtherTypeArray(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadArray(type);
                case HproseTags.TagRef: return (Array)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public BitArray ReadBitArray()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        BitArray a = new BitArray(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a[i] = ReadBoolean();
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (BitArray)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBitArray);
            }
        }

        public ArrayList ReadArrayList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        ArrayList a = new ArrayList(count);

                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(Unserialize());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (ArrayList)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofArrayList);
            }
        }

        public Queue ReadQueue()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Queue a = new Queue(count);

                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Enqueue(Unserialize());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (Queue)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofQueue);
            }
        }

        public Stack ReadStack()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Stack a = new Stack(count);

                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Push(Unserialize());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (Stack)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStack);
            }
        }

        public IList ReadIList(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        IList a = (IList)HproseHelper.NewInstance(type);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(Unserialize());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (IList)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public List<T> ReadList<T>()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<T> a = new List<T>(count);
                        refer.Set(a);
                        Type type = typeof(T);
                        TypeEnum typeEnum = HproseHelper.GetTypeEnum(type);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add((T)Unserialize(type, typeEnum));
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<T>)ReadRef();
                default: throw CastError(TagToString(tag), typeof(List<T>));
            }
        }

        public List<object> ReadObjectList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<object> a = new List<object>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(Unserialize());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofObjectList);
            }
        }

        public List<bool> ReadBooleanList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<bool> a = new List<bool>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadBoolean());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<bool>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBooleanList);
            }
        }

        public List<char> ReadCharList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<char> a = new List<char>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadChar());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<char>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofCharList);
            }
        }


        public List<sbyte> ReadSByteList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<sbyte> a = new List<sbyte>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadSByte());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<sbyte>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofSByteList);
            }
        }

        public List<byte> ReadByteList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<byte> a = new List<byte>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadByte());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<byte>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofByteList);
            }
        }

        public List<short> ReadInt16List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<short> a = new List<short>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadInt16());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<short>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt16List);
            }
        }


        public List<ushort> ReadUInt16List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<ushort> a = new List<ushort>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadUInt16());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<ushort>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt16List);
            }
        }

        public List<int> ReadInt32List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<int> a = new List<int>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadInt32());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<int>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt32List);
            }
        }


        public List<uint> ReadUInt32List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<uint> a = new List<uint>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadUInt32());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<uint>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt32List);
            }
        }

        public List<long> ReadInt64List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<long> a = new List<long>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadInt64());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<long>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofInt64List);
            }
        }


        public List<ulong> ReadUInt64List()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<ulong> a = new List<ulong>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadUInt64());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<ulong>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofUInt64List);
            }
        }

        public List<IntPtr> ReadIntPtrList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<IntPtr> a = new List<IntPtr>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadIntPtr());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<IntPtr>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofIntPtrList);
            }
        }

        public List<float> ReadSingleList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<float> a = new List<float>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadSingle());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<float>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofSingleList);
            }
        }

        public List<double> ReadDoubleList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<double> a = new List<double>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadDouble());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<double>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDoubleList);
            }
        }

        public List<decimal> ReadDecimalList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<decimal> a = new List<decimal>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadDecimal());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<decimal>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDecimalList);
            }
        }

        public List<DateTime> ReadDateTimeList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<DateTime> a = new List<DateTime>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadDateTime());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<DateTime>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofDateTimeList);
            }
        }

        public List<string> ReadStringList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<string> a = new List<string>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadString());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<string>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringList);
            }
        }

        public List<StringBuilder> ReadStringBuilderList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<StringBuilder> a = new List<StringBuilder>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadStringBuilder());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<StringBuilder>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringBuilderList);
            }
        }

        public List<Guid> ReadGuidList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<Guid> a = new List<Guid>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadGuid());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<Guid>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofGuidList);
            }
        }

        public List<BigInteger> ReadBigIntegerList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<BigInteger> a = new List<BigInteger>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadBigInteger());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<BigInteger>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBigIntegerList);
            }
        }

        public List<TimeSpan> ReadTimeSpanList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<TimeSpan> a = new List<TimeSpan>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadTimeSpan());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<TimeSpan>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofTimeSpanList);
            }
        }

        public List<char[]> ReadCharsList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<char[]> a = new List<char[]>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadCharArray());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<char[]>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofCharsList);
            }
        }

        public List<byte[]> ReadBytesList()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        List<byte[]> a = new List<byte[]>(count);
                        refer.Set(a);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add(ReadByteArray());
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (List<byte[]>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofBytesList);
            }
        }

        public Queue<T> ReadQueue<T>()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Queue<T> a = new Queue<T>(count);
                        refer.Set(a);
                        Type type = typeof(T);
                        TypeEnum typeEnum = HproseHelper.GetTypeEnum(type);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Enqueue((T)Unserialize(type, typeEnum));
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (Queue<T>)ReadRef();
                default: throw CastError(TagToString(tag), typeof(Queue<T>));
            }
        }

        public Stack<T> ReadStack<T>()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Stack<T> a = new Stack<T>(count);
                        refer.Set(a);
                        Type type = typeof(T);
                        TypeEnum typeEnum = HproseHelper.GetTypeEnum(type);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Push((T)Unserialize(type, typeEnum));
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (Stack<T>)ReadRef();
                default: throw CastError(TagToString(tag), typeof(Stack<T>));
            }
        }

        public IList<T> ReadIList<T>(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        if (type == typeof(IList<T>)) type = typeof(List<T>);
                        IList<T> a = (IList<T>)HproseHelper.NewInstance(type);
                        refer.Set(a);
                        Type t = typeof(T);
                        TypeEnum typeEnum = HproseHelper.GetTypeEnum(t);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add((T)Unserialize(t, typeEnum));
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (IList<T>)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public ICollection<T> ReadICollection<T>(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        if (type == typeof(ICollection<T>)) type = typeof(List<T>);
                        ICollection<T> a = (ICollection<T>)HproseHelper.NewInstance(type);
                        refer.Set(a);
                        Type t = typeof(T);
                        TypeEnum typeEnum = HproseHelper.GetTypeEnum(t);
                        for (int i = 0; i < count; ++i)
                        {
                            a.Add((T)Unserialize(t, typeEnum));
                        }
                        stream.ReadByte();
                        return a;
                    }
                case HproseTags.TagRef: return (ICollection<T>)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public HashMap ReadHashMap()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap map = new HashMap(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = i;
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap map = new HashMap(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = Unserialize();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadHashMap();
                    }
                case HproseTags.TagObject: return (HashMap)ReadObjectAsMap(new HashMap());
                case HproseTags.TagRef: return (HashMap)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofHashMap);
            }
        }

        public IDictionary ReadIDictionary(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        IDictionary map = (IDictionary)HproseHelper.NewInstance(type);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = i;
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        IDictionary map = (IDictionary)HproseHelper.NewInstance(type);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = Unserialize();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadIDictionary(type);
                    }
                case HproseTags.TagObject:
                    {
                        IDictionary map = (IDictionary)HproseHelper.NewInstance(type);
                        return (IDictionary)ReadObjectAsMap(map);
                    }
                case HproseTags.TagRef: return (IDictionary)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        private HashMap<TKey, TValue> ReadListAsHashMap<TKey, TValue>()
        {
            int count = ReadInt(HproseTags.TagOpenbrace);
            HashMap<TKey, TValue> map = new HashMap<TKey, TValue>(count);
            refer.Set(map);
            Type keyType = typeof(TKey);
            Type valueType = typeof(TValue);
            TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
            for (int i = 0; i < count; ++i)
            {
                TKey key = (TKey)Convert.ChangeType(i, keyType, null);
                TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                map[key] = value;
            }
            stream.ReadByte();
            return map;
        }

        public HashMap<TKey, TValue> ReadHashMap<TKey, TValue>()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsHashMap<TKey, TValue>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap<TKey, TValue> map = new HashMap<TKey, TValue>(count);
                        refer.Set(map);
                        Type keyType = typeof(TKey);
                        TypeEnum keyTypeEnum = HproseHelper.GetTypeEnum(keyType);
                        Type valueType = typeof(TValue);
                        TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
                        for (int i = 0; i < count; ++i)
                        {
                            TKey key = (TKey)Unserialize(keyType, keyTypeEnum);
                            TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagRef: return (HashMap<TKey, TValue>)ReadRef();
                default: throw CastError(TagToString(tag), typeof(HashMap<TKey, TValue>));
            }
        }

        public HashMap<string, object> ReadStringObjectHashMap()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsHashMap<string, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap<string, object> map = new HashMap<string, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            string key = ReadString();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadStringObjectHashMap();
                    }
                case HproseTags.TagObject: return (HashMap<string, object>)ReadObjectAsMap(new HashMap<string, object>());
                case HproseTags.TagRef: return (HashMap<string, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringObjectHashMap);
            }
        }

        public HashMap<object, object> ReadObjectObjectHashMap()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsHashMap<object, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap<object, object> map = new HashMap<object, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = Unserialize();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadObjectObjectHashMap();
                    }
                case HproseTags.TagObject: return (HashMap<object, object>)ReadObjectAsMap(new HashMap<object, object>());
                case HproseTags.TagRef: return (HashMap<object, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofObjectObjectHashMap);
            }
        }

        public HashMap<int, object> ReadIntObjectHashMap()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsHashMap<int, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        HashMap<int, object> map = new HashMap<int, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            int key = ReadInt32();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagRef: return (HashMap<int, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofIntObjectHashMap);
            }
        }

        private Dictionary<TKey, TValue> ReadListAsDictionary<TKey, TValue>()
        {
            int count = ReadInt(HproseTags.TagOpenbrace);
            Dictionary<TKey, TValue> map = new Dictionary<TKey, TValue>(count);
            refer.Set(map);
            Type keyType = typeof(TKey);
            Type valueType = typeof(TValue);
            TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
            for (int i = 0; i < count; ++i)
            {
                TKey key = (TKey)Convert.ChangeType(i, keyType, null);
                TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                map[key] = value;
            }
            stream.ReadByte();
            return map;
        }

        public Dictionary<TKey, TValue> ReadDictionary<TKey, TValue>()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsDictionary<TKey, TValue>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Dictionary<TKey, TValue> map = new Dictionary<TKey, TValue>(count);
                        refer.Set(map);
                        Type keyType = typeof(TKey);
                        TypeEnum keyTypeEnum = HproseHelper.GetTypeEnum(keyType);
                        Type valueType = typeof(TValue);
                        TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
                        for (int i = 0; i < count; ++i)
                        {
                            TKey key = (TKey)Unserialize(keyType, keyTypeEnum);
                            TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagRef: return (Dictionary<TKey, TValue>)ReadRef();
                default: throw CastError(TagToString(tag), typeof(Dictionary<TKey, TValue>));
            }
        }

        public Dictionary<string, object> ReadStringObjectDictionary()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsDictionary<string, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Dictionary<string, object> map = new Dictionary<string, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            string key = ReadString();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadStringObjectDictionary();
                    }
                case HproseTags.TagObject: return (Dictionary<string, object>)ReadObjectAsMap(new Dictionary<string, object>());
                case HproseTags.TagRef: return (Dictionary<string, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofStringObjectDictionary);
            }
        }

        public Dictionary<object, object> ReadObjectObjectDictionary()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsDictionary<object, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Dictionary<object, object> map = new Dictionary<object, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            object key = Unserialize();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagClass:
                    {
                        ReadClass();
                        return ReadObjectObjectDictionary();
                    }
                case HproseTags.TagObject: return (Dictionary<object, object>)ReadObjectAsMap(new Dictionary<object, object>());
                case HproseTags.TagRef: return (Dictionary<object, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofObjectObjectDictionary);
            }
        }

        public Dictionary<int, object> ReadIntObjectDictionary()
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsDictionary<int, object>();
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        Dictionary<int, object> map = new Dictionary<int, object>(count);
                        refer.Set(map);
                        for (int i = 0; i < count; ++i)
                        {
                            int key = ReadInt32();
                            object value = Unserialize();
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagRef: return (Dictionary<int, object>)ReadRef();
                default: throw CastError(TagToString(tag), HproseHelper.typeofIntObjectDictionary);
            }
        }

        private IDictionary<TKey, TValue> ReadListAsIDictionary<TKey, TValue>(Type type)
        {
            int count = ReadInt(HproseTags.TagOpenbrace);
            if (type == typeof(IDictionary<TKey, TValue>)) type = typeof(Dictionary<TKey, TValue>);
            IDictionary<TKey, TValue> map = (IDictionary<TKey, TValue>)HproseHelper.NewInstance(type);
            refer.Set(map);
            Type keyType = typeof(TKey);
            Type valueType = typeof(TValue);
            TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
            for (int i = 0; i < count; ++i)
            {
                TKey key = (TKey)Convert.ChangeType(i, keyType, null);
                TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                map[key] = value;
            }
            stream.ReadByte();
            return map;
        }

        public IDictionary<TKey, TValue> ReadIDictionary<TKey, TValue>(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagList: return ReadListAsIDictionary<TKey, TValue>(type);
                case HproseTags.TagMap:
                    {
                        int count = ReadInt(HproseTags.TagOpenbrace);
                        if (type == typeof(IDictionary<TKey, TValue>)) type = typeof(Dictionary<TKey, TValue>);
                        IDictionary<TKey, TValue> map = (IDictionary<TKey, TValue>)HproseHelper.NewInstance(type);
                        refer.Set(map);
                        Type keyType = typeof(TKey);
                        TypeEnum keyTypeEnum = HproseHelper.GetTypeEnum(keyType);
                        Type valueType = typeof(TValue);
                        TypeEnum valueTypeEnum = HproseHelper.GetTypeEnum(valueType);
                        for (int i = 0; i < count; ++i)
                        {
                            TKey key = (TKey)Unserialize(keyType, keyTypeEnum);
                            TValue value = (TValue)Unserialize(valueType, valueTypeEnum);
                            map[key] = value;
                        }
                        stream.ReadByte();
                        return map;
                    }
                case HproseTags.TagRef: return (IDictionary<TKey, TValue>)ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }


        public object ReadObject(Type type)
        {
            int tag = stream.ReadByte();
            switch (tag)
            {
                case HproseTags.TagNull: return null;
                case HproseTags.TagMap: return ReadMapAsObject(type);
                case HproseTags.TagClass: ReadClass(); return ReadObject(type);
                case HproseTags.TagObject: return ReadObjectWithoutTag(type);
                case HproseTags.TagRef: return ReadRef(type);
                default: throw CastError(TagToString(tag), type);
            }
        }

        public object Unserialize(Type type)
        {
            return Unserialize(type, HproseHelper.GetTypeEnum(type));
        }

        private object Unserialize(Type type, TypeEnum typeEnum)
        {
            switch (typeEnum)
            {
                case TypeEnum.Null: return Unserialize();
                case TypeEnum.Object: return ReadObject();
                case TypeEnum.DBNull: return ReadDBNull();
                case TypeEnum.Boolean: return ReadBoolean();
                case TypeEnum.Char: return ReadChar();
                case TypeEnum.SByte: return ReadSByte();
                case TypeEnum.Byte: return ReadByte();
                case TypeEnum.Int16: return ReadInt16();
                case TypeEnum.UInt16: return ReadUInt16();
                case TypeEnum.Int32: return ReadInt32();
                case TypeEnum.UInt32: return ReadUInt32();
                case TypeEnum.Int64: return ReadInt64();
                case TypeEnum.UInt64: return ReadUInt64();
                case TypeEnum.IntPtr: return ReadIntPtr();
                case TypeEnum.Single: return ReadSingle();
                case TypeEnum.Double: return ReadDouble();
                case TypeEnum.DateTime: return ReadDateTime();
                case TypeEnum.String: return ReadString();
                case TypeEnum.IpAddress: return new IPAddress(ReadByteArray());
                case TypeEnum.StringBuilder: return ReadStringBuilder();
                case TypeEnum.Guid: return ReadGuid();
                case TypeEnum.BigInteger: return ReadBigInteger();
                case TypeEnum.TimeSpan: return ReadTimeSpan();
                case TypeEnum.Stream:
                case TypeEnum.MemoryStream: return ReadStream();
                case TypeEnum.ObjectArray: return ReadObjectArray();
                case TypeEnum.BooleanArray: return ReadBooleanArray();
                case TypeEnum.CharArray: return ReadCharArray();
                case TypeEnum.SByteArray: return ReadSByteArray();
                case TypeEnum.ByteArray: return ReadByteArray();
                case TypeEnum.Int16Array: return ReadInt16Array();
                case TypeEnum.UInt16Array: return ReadUInt16Array();
                case TypeEnum.Int32Array: return ReadInt32Array();
                case TypeEnum.UInt32Array: return ReadUInt32Array();
                case TypeEnum.Int64Array: return ReadInt64Array();
                case TypeEnum.UInt64Array: return ReadUInt64Array();
                case TypeEnum.IntPtrArray: return ReadIntPtrArray();
                case TypeEnum.SingleArray: return ReadSingleArray();
                case TypeEnum.DoubleArray: return ReadDoubleArray();
                case TypeEnum.DateTimeArray: return ReadDateTimeArray();
                case TypeEnum.StringArray: return ReadStringArray();
                case TypeEnum.StringBuilderArray: return ReadStringBuilderArray();
                case TypeEnum.GuidArray: return ReadGuidArray();
                case TypeEnum.BigIntegerArray: return ReadBigIntegerArray();
                case TypeEnum.TimeSpanArray: return ReadTimeSpanArray();
                case TypeEnum.CharsArray: return ReadCharsArray();
                case TypeEnum.BytesArray: return ReadBytesArray();
                case TypeEnum.OtherTypeArray: return ReadOtherTypeArray(type);
                case TypeEnum.Decimal: return ReadDecimal();
                case TypeEnum.Enum: return ReadEnum(type);
                case TypeEnum.DecimalArray: return ReadDecimalArray();
                case TypeEnum.BitArray: return ReadBitArray();
                case TypeEnum.ArrayList: return ReadArrayList();
                case TypeEnum.Queue: return ReadQueue();
                case TypeEnum.Stack: return ReadStack();
                case TypeEnum.Hashtable:
                case TypeEnum.HashMap: return ReadHashMap();
                case TypeEnum.NullableBoolean: return ReadNullableBoolean();
                case TypeEnum.NullableChar: return ReadNullableChar();
                case TypeEnum.NullableSByte: return ReadNullableSByte();
                case TypeEnum.NullableByte: return ReadNullableByte();
                case TypeEnum.NullableInt16: return ReadNullableInt16();
                case TypeEnum.NullableUInt16: return ReadNullableUInt16();
                case TypeEnum.NullableInt32: return ReadNullableInt32();
                case TypeEnum.NullableUInt32: return ReadNullableUInt32();
                case TypeEnum.NullableInt64: return ReadNullableInt64();
                case TypeEnum.NullableUInt64: return ReadNullableUInt64();
                case TypeEnum.NullableIntPtr: return ReadNullableIntPtr();
                case TypeEnum.NullableSingle: return ReadNullableSingle();
                case TypeEnum.NullableDouble: return ReadNullableDouble();
                case TypeEnum.NullableDecimal: return ReadNullableDecimal();
                case TypeEnum.NullableDateTime: return ReadNullableDateTime();
                case TypeEnum.NullableGuid: return ReadNullableGuid();
                case TypeEnum.NullableBigInteger: return ReadNullableBigInteger();
                case TypeEnum.NullableTimeSpan: return ReadNullableTimeSpan();
                case TypeEnum.ObjectList:
                case TypeEnum.ObjectIList: return ReadObjectList();
                case TypeEnum.BooleanList:
                case TypeEnum.BooleanIList: return ReadBooleanList();
                case TypeEnum.CharList:
                case TypeEnum.CharIList: return ReadCharList();
                case TypeEnum.SByteList:
                case TypeEnum.SByteIList: return ReadSByteList();
                case TypeEnum.ByteList:
                case TypeEnum.ByteIList: return ReadByteList();
                case TypeEnum.Int16List:
                case TypeEnum.Int16IList: return ReadInt16List();
                case TypeEnum.UInt16List:
                case TypeEnum.UInt16IList: return ReadUInt16List();
                case TypeEnum.Int32List:
                case TypeEnum.Int32IList: return ReadInt32List();
                case TypeEnum.UInt32List:
                case TypeEnum.UInt32IList: return ReadUInt32List();
                case TypeEnum.Int64List:
                case TypeEnum.Int64IList: return ReadInt64List();
                case TypeEnum.UInt64List:
                case TypeEnum.UInt64IList: return ReadUInt64List();
                case TypeEnum.IntPtrList:
                case TypeEnum.IntPtrIList: return ReadIntPtrList();
                case TypeEnum.SingleList:
                case TypeEnum.SingleIList: return ReadSingleList();
                case TypeEnum.DoubleList:
                case TypeEnum.DoubleIList: return ReadDoubleList();
                case TypeEnum.DecimalList:
                case TypeEnum.DecimalIList: return ReadDecimalList();
                case TypeEnum.DateTimeList:
                case TypeEnum.DateTimeIList: return ReadDateTimeList();
                case TypeEnum.StringList:
                case TypeEnum.StringIList: return ReadStringList();
                case TypeEnum.StringBuilderList:
                case TypeEnum.StringBuilderIList: return ReadStringBuilderList();
                case TypeEnum.GuidList:
                case TypeEnum.GuidIList: return ReadGuidList();
                case TypeEnum.BigIntegerList:
                case TypeEnum.BigIntegerIList: return ReadBigIntegerList();
                case TypeEnum.TimeSpanList:
                case TypeEnum.TimeSpanIList: return ReadTimeSpanList();
                case TypeEnum.CharsList:
                case TypeEnum.CharsIList: return ReadCharsList();
                case TypeEnum.BytesList:
                case TypeEnum.BytesIList: return ReadBytesList();
                case TypeEnum.StringObjectHashMap: return ReadStringObjectHashMap();
                case TypeEnum.ObjectObjectHashMap: return ReadObjectObjectHashMap();
                case TypeEnum.IntObjectHashMap: return ReadIntObjectHashMap();
                case TypeEnum.StringObjectDictionary: return ReadStringObjectDictionary();
                case TypeEnum.ObjectObjectDictionary: return ReadObjectObjectDictionary();
                case TypeEnum.IntObjectDictionary: return ReadIntObjectDictionary();
                case TypeEnum.GenericList: return HproseHelper.GetIGListReader(type).ReadList(this);
                case TypeEnum.GenericDictionary: return HproseHelper.GetIGDictionaryReader(type).ReadDictionary(this);
                case TypeEnum.GenericQueue: return HproseHelper.GetIGQueueReader(type).ReadQueue(this);
                case TypeEnum.GenericStack: return HproseHelper.GetIGStackReader(type).ReadStack(this);
                case TypeEnum.GenericIList: return HproseHelper.GetIGIListReader(type).ReadIList(this, type);
                case TypeEnum.GenericICollection: return HproseHelper.GetIGICollectionReader(type).ReadICollection(this, type);
                case TypeEnum.GenericIDictionary: return HproseHelper.GetIGIDictionaryReader(type).ReadIDictionary(this, type);
                case TypeEnum.ICollection:
                case TypeEnum.IList: return ReadObjectList();
                case TypeEnum.IDictionary: return ReadObjectObjectHashMap();

                case TypeEnum.List: return ReadIList(type);
                case TypeEnum.Dictionary: return ReadIDictionary(type);
                case TypeEnum.OtherType: return ReadObject(type);


            }
            throw new HproseException("Can not unserialize this type: " + type.FullName);
        }

        public MemoryStream ReadRaw()
        {
            MemoryStream ostream = new MemoryStream();
            ReadRaw(ostream);
            return ostream;
        }

        public void ReadRaw(Stream ostream)
        {
            ReadRaw(ostream, stream.ReadByte());
        }

        private void ReadRaw(Stream ostream, int tag)
        {
            ostream.WriteByte((byte)tag);
            switch (tag)
            {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case HproseTags.TagNull:
                case HproseTags.TagEmpty:
                case HproseTags.TagTrue:
                case HproseTags.TagFalse:
                case HproseTags.TagNaN:
                    break;
                case HproseTags.TagInfinity:
                    ostream.WriteByte((byte)stream.ReadByte());
                    break;
                case HproseTags.TagInteger:
                case HproseTags.TagLong:
                case HproseTags.TagDouble:
                case HproseTags.TagRef:
                    ReadNumberRaw(ostream);
                    break;
                case HproseTags.TagDate:
                case HproseTags.TagTime:
                    ReadDateTimeRaw(ostream);
                    break;
                case HproseTags.TagUTF8Char:
                    ReadUTF8CharRaw(ostream);
                    break;
                case HproseTags.TagBytes:
                    ReadBytesRaw(ostream);
                    break;
                case HproseTags.TagString:
                    ReadStringRaw(ostream);
                    break;
                case HproseTags.TagGuid:
                    ReadGuidRaw(ostream);
                    break;
                case HproseTags.TagList:
                case HproseTags.TagMap:
                case HproseTags.TagObject:
                    ReadComplexRaw(ostream);
                    break;
                case HproseTags.TagClass:
                    ReadComplexRaw(ostream);
                    ReadRaw(ostream);
                    break;
                case HproseTags.TagError:
                    ReadRaw(ostream);
                    break;
                default: throw UnexpectedTag(tag);
            }
        }

        private void ReadNumberRaw(Stream ostream)
        {
            int tag;
            do
            {
                tag = stream.ReadByte();
                ostream.WriteByte((byte)tag);
            } while (tag != HproseTags.TagSemicolon);
        }

        private void ReadDateTimeRaw(Stream ostream)
        {
            int tag;
            do
            {
                tag = stream.ReadByte();
                ostream.WriteByte((byte)tag);
            } while (tag != HproseTags.TagSemicolon &&
                     tag != HproseTags.TagUTC);
        }

        private void ReadUTF8CharRaw(Stream ostream)
        {
            int tag = stream.ReadByte();
            switch (tag >> 4)
            {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    {
                        // 0xxx xxxx
                        ostream.WriteByte((byte)tag);
                        break;
                    }
                case 12:
                case 13:
                    {
                        // 110x xxxx   10xx xxxx
                        ostream.WriteByte((byte)tag);
                        ostream.WriteByte((byte)stream.ReadByte());
                        break;
                    }
                case 14:
                    {
                        // 1110 xxxx  10xx xxxx  10xx xxxx
                        ostream.WriteByte((byte)tag);
                        ostream.WriteByte((byte)stream.ReadByte());
                        ostream.WriteByte((byte)stream.ReadByte());
                        break;
                    }
                default:
                    throw new HproseException("bad utf-8 encoding at " +
                                              ((tag < 0) ? "end of stream" :
                                                  "0x" + (tag & 0xff).ToString("x2")));
            }
        }

        private void ReadBytesRaw(Stream ostream)
        {
            int len = 0;
            int tag = '0';
            do
            {
                len *= 10;
                len += tag - '0';
                tag = stream.ReadByte();
                ostream.WriteByte((byte)tag);
            } while (tag != HproseTags.TagQuote);
            int off = 0;
            byte[] b = new byte[len];
            while (len > 0)
            {
                int size = stream.Read(b, off, len);
                off += size;
                len -= size;
            }
            ostream.Write(b, 0, b.Length);
            ostream.WriteByte((byte)stream.ReadByte());
        }

        private void ReadStringRaw(Stream ostream)
        {
            int count = 0;
            int tag = '0';
            do
            {
                count *= 10;
                count += tag - '0';
                tag = stream.ReadByte();
                ostream.WriteByte((byte)tag);
            } while (tag != HproseTags.TagQuote);
            for (int i = 0; i < count; ++i)
            {
                tag = stream.ReadByte();
                switch (tag >> 4)
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        {
                            // 0xxx xxxx
                            ostream.WriteByte((byte)tag);
                            break;
                        }
                    case 12:
                    case 13:
                        {
                            // 110x xxxx   10xx xxxx
                            ostream.WriteByte((byte)tag);
                            ostream.WriteByte((byte)stream.ReadByte());
                            break;
                        }
                    case 14:
                        {
                            // 1110 xxxx  10xx xxxx  10xx xxxx
                            ostream.WriteByte((byte)tag);
                            ostream.WriteByte((byte)stream.ReadByte());
                            ostream.WriteByte((byte)stream.ReadByte());
                            break;
                        }
                    case 15:
                        {
                            // 1111 0xxx  10xx xxxx  10xx xxxx  10xx xxxx
                            if ((tag & 0xf) <= 4)
                            {
                                ostream.WriteByte((byte)tag);
                                ostream.WriteByte((byte)stream.ReadByte());
                                ostream.WriteByte((byte)stream.ReadByte());
                                ostream.WriteByte((byte)stream.ReadByte());
                                ++i;
                                break;
                            }
                            goto default;
                            // no break here!! here need throw exception.
                        }
                    default:
                        throw new HproseException("bad utf-8 encoding at " +
                                                  ((tag < 0) ? "end of stream" :
                                                      "0x" + (tag & 0xff).ToString("x2")));
                }
            }
            ostream.WriteByte((byte)stream.ReadByte());
        }

        private void ReadGuidRaw(Stream ostream)
        {
            int len = 38;
            int off = 0;
            byte[] b = new byte[len];
            while (len > 0)
            {
                int size = stream.Read(b, off, len);
                off += size;
                len -= size;
            }
            ostream.Write(b, 0, b.Length);
        }

        private void ReadComplexRaw(Stream ostream)
        {
            int tag;
            do
            {
                tag = stream.ReadByte();
                ostream.WriteByte((byte)tag);
            } while (tag != HproseTags.TagOpenbrace);
            while ((tag = stream.ReadByte()) != HproseTags.TagClosebrace)
            {
                ReadRaw(ostream, tag);
            }
            ostream.WriteByte((byte)tag);
        }

        public void Reset()
        {
            refer.Reset();
            classref.Clear();
            membersref.Clear();
        }
    }
    internal sealed class HproseTags
    {
        /* Serialize Tags */
        public const int TagInteger = 'i';
        public const int TagLong = 'l';
        public const int TagDouble = 'd';
        public const int TagNull = 'n';
        public const int TagEmpty = 'e';
        public const int TagTrue = 't';
        public const int TagFalse = 'f';
        public const int TagNaN = 'N';
        public const int TagInfinity = 'I';
        public const int TagDate = 'D';
        public const int TagTime = 'T';
        public const int TagUTC = 'Z';
        public const int TagBytes = 'b';
        public const int TagUTF8Char = 'u';
        public const int TagString = 's';
        public const int TagGuid = 'g';
        public const int TagList = 'a';
        public const int TagMap = 'm';
        public const int TagClass = 'c';
        public const int TagObject = 'o';
        public const int TagRef = 'r';
        /* Serialize Marks */
        public const int TagPos = '+';
        public const int TagNeg = '-';
        public const int TagSemicolon = ';';
        public const int TagOpenbrace = '{';
        public const int TagClosebrace = '}';
        public const int TagQuote = '"';
        public const int TagPoint = '.';
        /* Protocol Tags */
        public const int TagFunctions = 'F';
        public const int TagCall = 'C';
        public const int TagResult = 'R';
        public const int TagArgument = 'A';
        public const int TagError = 'E';
        public const int TagEnd = 'z';
    }
    internal class IdentityEqualityComparer : IEqualityComparer<object>
    {
        bool IEqualityComparer<object>.Equals(object x, object y)
        {
            return object.ReferenceEquals(x, y);
        }

        int IEqualityComparer<object>.GetHashCode(object obj)
        {
            return obj.GetHashCode();
        }
    }
    internal interface WriterRefer
    {
        void AddCount(int count);
        void Set(object obj);
        bool Write(object obj);
        void Reset();
    }

    internal sealed class FakeWriterRefer : WriterRefer
    {
        public void AddCount(int count) { }
        public void Set(object obj) { }
        public bool Write(object obj)
        {
            return false;
        }
        public void Reset() { }
    }

    internal sealed class RealWriterRefer : WriterRefer
    {
        private HproseWriter writer;
        private Dictionary<object, int> references;

        private int lastref = 0;
        public RealWriterRefer(HproseWriter writer, HproseMode mode)
        {

            this.writer = writer;
            if (mode == HproseMode.FieldMode)
            {
                references = new Dictionary<object, int>(new IdentityEqualityComparer());
            }
            else
            {
                references = new Dictionary<object, int>();
            }

        }
        public void AddCount(int count)
        {
            lastref += count;
        }
        public void Set(object obj)
        {
            references[obj] = lastref++;
        }
        public bool Write(object obj)
        {
            if (references.ContainsKey(obj))
            {

                writer.stream.WriteByte(HproseTags.TagRef);
                writer.WriteInt(references[obj], writer.stream);

                writer.stream.WriteByte(HproseTags.TagSemicolon);
                return true;
            }
            return false;
        }
        public void Reset()
        {
            references.Clear();
            lastref = 0;
        }
    }

    internal sealed class HproseWriter
    {
        public Stream stream;
        private HproseMode mode;
        private static Dictionary<Type, SerializeCache> fieldsCache = new Dictionary<Type, SerializeCache>();
        private static Dictionary<Type, SerializeCache> propertiesCache = new Dictionary<Type, SerializeCache>();
        private static Dictionary<Type, SerializeCache> membersCache = new Dictionary<Type, SerializeCache>();
        private Dictionary<Type, int> classref = new Dictionary<Type, int>();

        private WriterRefer refer;
        private byte[] buf = new byte[20];
        private static byte[] minIntBuf = new byte[] {(byte)'-',(byte)'2',(byte)'1',(byte)'4',(byte)'7',(byte)'4',
                                                        (byte)'8',(byte)'3',(byte)'6',(byte)'4',(byte)'8'};
        private static byte[] minLongBuf = new byte[] {(byte)'-',(byte)'9',(byte)'2',(byte)'2',(byte)'3',
                                                         (byte)'3',(byte)'7',(byte)'2',(byte)'0',(byte)'3',
                                                         (byte)'6',(byte)'8',(byte)'5',(byte)'4',(byte)'7',
                                                         (byte)'7',(byte)'5',(byte)'8',(byte)'0',(byte)'8'};
        private int lastclassref = 0;

        public HproseWriter(Stream stream)
            : this(stream, HproseMode.MemberMode, false)
        {
        }

        public HproseWriter(Stream stream, bool simple)
            : this(stream, HproseMode.MemberMode, simple)
        {
        }

        public HproseWriter(Stream stream, HproseMode mode)
            : this(stream, mode, false)
        {
        }

        public HproseWriter(Stream stream, HproseMode mode, bool simple)
        {
            this.stream = stream;
            this.mode = mode;
            this.refer = (simple ? new FakeWriterRefer() as WriterRefer : new RealWriterRefer(this, mode) as WriterRefer);
        }


        public void Serialize(object obj)
        {
            if (obj == null) WriteNull();
            else if (obj is ValueType)
            {
                if (obj is int) WriteInteger((int)obj);
                else if (obj is double) WriteDouble((double)obj);
                else if (obj is bool) WriteBoolean((bool)obj);
                else if (obj is char) WriteUTF8Char((char)obj);
                else if (obj is byte) WriteInteger((byte)obj);
                else if (obj is sbyte) WriteInteger((sbyte)obj);
                else if (obj is ushort) WriteInteger((ushort)obj);
                else if (obj is short) WriteInteger((short)obj);
                else if (obj is uint) WriteLong((uint)obj);
                else if (obj is ulong) WriteLong((ulong)obj);
                else if (obj is long) WriteLong((long)obj);
                else if (obj is IntPtr) WriteIntPtr((IntPtr)obj);
                else if (obj is float) WriteDouble((float)obj);
                else if (obj is decimal) WriteDouble((decimal)obj);
                else if (obj is DateTime) WriteDateWithRef((DateTime)obj);
                else if (obj is Enum) WriteEnum(obj, obj.GetType());
                else if (obj is TimeSpan) WriteLong(((TimeSpan)obj).Ticks);
                else if (obj is BigInteger) WriteLong((BigInteger)obj);
                else if (obj is Guid) WriteGuidWithRef((Guid)obj);
                else WriteObjectWithRef(obj);
            }
            else if (obj is IPAddress)
            {
                var bin = ((IPAddress)obj).GetAddressBytes();
                WriteBytesWithRef(bin);
            }
            else if (obj is String)
            {
                switch (((string)obj).Length)
                {
                    case 0: WriteEmpty(); break;
                    case 1: WriteUTF8Char(((string)obj)[0]); break;
                    default: WriteStringWithRef((string)obj); break;
                }
            }
            else if (obj is StringBuilder)
            {
                switch (((StringBuilder)obj).Length)
                {
                    case 0: WriteEmpty(); break;
                    case 1: WriteUTF8Char(((StringBuilder)obj)[0]); break;
                    default: WriteStringWithRef((StringBuilder)obj); break;
                }
            }
            else if (obj is Stream)
            {
                WriteStreamWithRef((Stream)obj);
            }
            else if (obj is Array)
            {
                switch ((TypeEnum)HproseHelper.GetArrayTypeEnum(obj.GetType()))
                {
                    case TypeEnum.ObjectArray: WriteArrayWithRef((object[])obj); break;
                    case TypeEnum.BooleanArray: WriteArrayWithRef((bool[])obj); break;
                    case TypeEnum.CharArray: WriteStringWithRef((char[])obj); break;
                    case TypeEnum.SByteArray: WriteArrayWithRef((sbyte[])obj); break;
                    case TypeEnum.ByteArray: WriteBytesWithRef((byte[])obj); break;
                    case TypeEnum.Int16Array: WriteArrayWithRef((short[])obj); break;
                    case TypeEnum.UInt16Array: WriteArrayWithRef((ushort[])obj); break;
                    case TypeEnum.Int32Array: WriteArrayWithRef((int[])obj); break;
                    case TypeEnum.UInt32Array: WriteArrayWithRef((uint[])obj); break;
                    case TypeEnum.Int64Array: WriteArrayWithRef((long[])obj); break;
                    case TypeEnum.UInt64Array: WriteArrayWithRef((ulong[])obj); break;
                    case TypeEnum.IntPtrArray: WriteArrayWithRef((IntPtr[])obj); break;
                    case TypeEnum.SingleArray: WriteArrayWithRef((float[])obj); break;
                    case TypeEnum.DoubleArray: WriteArrayWithRef((double[])obj); break;
                    case TypeEnum.DecimalArray: WriteArrayWithRef((decimal[])obj); break;
                    case TypeEnum.DateTimeArray: WriteArrayWithRef((DateTime[])obj); break;
                    case TypeEnum.StringArray: WriteArrayWithRef((string[])obj); break;
                    case TypeEnum.StringBuilderArray: WriteArrayWithRef((StringBuilder[])obj); break;
                    case TypeEnum.BigIntegerArray: WriteArrayWithRef((BigInteger[])obj); break;
                    case TypeEnum.TimeSpanArray: WriteArrayWithRef((TimeSpan[])obj); break;
                    case TypeEnum.GuidArray: WriteArrayWithRef((Guid[])obj); break;
                    case TypeEnum.BytesArray: WriteArrayWithRef((byte[][])obj); break;
                    case TypeEnum.CharsArray: WriteArrayWithRef((char[][])obj); break;
                    default: WriteArrayWithRef((Array)obj); break;
                }
            }
            else if (obj is ArrayList) WriteListWithRef((IList)obj);
            else if (obj is IList)
            {
                if (obj is IList<int>) WriteListWithRef((IList<int>)obj);
                else if (obj is IList<string>) WriteListWithRef((IList<string>)obj);
                else if (obj is IList<double>) WriteListWithRef((IList<double>)obj);
                else
                    WriteListWithRef((IList)obj);
            }
            else if (obj is IDictionary)
            {
                WriteMapWithRef((IDictionary)obj);
            }
            else if (obj is ICollection)
            {
                WriteCollectionWithRef((ICollection)obj);
            }
            else if (obj is DBNull)
            {
                WriteNull();
            }
            else if (obj is IEnumerable)
            {
                if (obj is IEnumerable<int>)
                {
                    var lst = (obj as IEnumerable).Cast<int>().ToList();
                    WriteListWithRef((IList<int>)lst);
                }
                else if (obj is IEnumerable<string>)
                {
                    var lst = (obj as IEnumerable).Cast<string>().ToList();
                    WriteListWithRef((IList<string>)lst);
                }
                else if (obj is IEnumerable<double>)
                {
                    var lst = (obj as IEnumerable).Cast<double>().ToList();
                    WriteListWithRef((IList<double>)lst);
                }
                else
                {
                    var lst = (obj as IEnumerable).Cast<object>().ToList();
                    WriteListWithRef((IList)lst);
                }
            }
            else
            {
                Type type = obj.GetType();
                Type typeinfo = type;

                if (typeinfo.IsGenericType && type.Name.StartsWith("<>f__AnonymousType"))
                {
                    WriteAnonymousTypeWithRef(obj);
                    return;
                }
                if (HproseHelper.typeofISerializable.IsAssignableFrom(type))
                {
                    throw new HproseException(type.Name + " is a ISerializable type, hprose can't support it.");
                }
                WriteObjectWithRef(obj);
            }
        }


        public void WriteInteger(sbyte i)
        {
            if (i >= 0 && i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                stream.WriteByte(HproseTags.TagInteger);
                WriteIntFast((int)i, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteInteger(short i)
        {
            if (i >= 0 && i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                stream.WriteByte(HproseTags.TagInteger);
                WriteIntFast((int)i, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteInteger(int i)
        {
            if (i >= 0 && i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                stream.WriteByte(HproseTags.TagInteger);
                if (i == Int32.MinValue)
                {
                    stream.Write(minIntBuf, 0, minIntBuf.Length);
                }
                else
                {
                    WriteIntFast(i, stream);
                }
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteInteger(byte i)
        {
            if (i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                stream.WriteByte(HproseTags.TagInteger);
                WriteIntFast((uint)i, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }


        public void WriteInteger(ushort i)
        {
            if (i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                stream.WriteByte(HproseTags.TagInteger);
                WriteIntFast((uint)i, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }


        public void WriteLong(uint l)
        {
            if (l <= 9)
            {
                stream.WriteByte((byte)('0' + l));
            }
            else
            {
                stream.WriteByte(HproseTags.TagLong);
                WriteIntFast((uint)l, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteLong(long l)
        {
            if (l >= 0 && l <= 9)
            {
                stream.WriteByte((byte)('0' + l));
            }
            else
            {
                stream.WriteByte(HproseTags.TagLong);
                if (l == Int64.MinValue)
                {
                    stream.Write(minLongBuf, 0, minLongBuf.Length);
                }
                else
                {
                    WriteIntFast((long)l, stream);
                }
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }


        public void WriteLong(ulong l)
        {
            if (l <= 9)
            {
                stream.WriteByte((byte)('0' + l));
            }
            else
            {
                stream.WriteByte(HproseTags.TagLong);
                WriteIntFast((ulong)l, stream);
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteIntPtr(IntPtr ip)
        {
            if (IntPtr.Size == 4)
            {
                WriteInteger(ip.ToInt32());
            }
            else
            {
                WriteLong(ip.ToInt64());
            }
        }

        public void WriteLong(BigInteger l)
        {
            stream.WriteByte(HproseTags.TagLong);
            WriteAsciiString(l.ToString());
            stream.WriteByte(HproseTags.TagSemicolon);
        }

        public void WriteEnum(object value, Type type)
        {
            switch (HproseHelper.GetTypeEnum(Enum.GetUnderlyingType(type)))
            {

                case TypeEnum.Int32: WriteInteger((int)value); break;
                case TypeEnum.Byte: WriteInteger((byte)value); break;
                case TypeEnum.SByte: WriteInteger((sbyte)value); break;
                case TypeEnum.Int16: WriteInteger((short)value); break;
                case TypeEnum.UInt16: WriteInteger((ushort)value); break;
                case TypeEnum.UInt32: WriteLong((uint)value); break;
                case TypeEnum.Int64: WriteLong((long)value); break;
                case TypeEnum.UInt64: WriteLong((ulong)value); break;
            }
        }

        public void WriteDouble(float d)
        {
            if (double.IsNaN(d))
            {
                stream.WriteByte(HproseTags.TagNaN);
            }
            else if (double.IsInfinity(d))
            {
                stream.WriteByte(HproseTags.TagInfinity);
                if (d > 0)
                {
                    stream.WriteByte(HproseTags.TagPos);
                }
                else
                {
                    stream.WriteByte(HproseTags.TagNeg);
                }
            }
            else
            {
                stream.WriteByte(HproseTags.TagDouble);
                WriteAsciiString(d.ToString("R"));
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteDouble(double d)
        {
            if (double.IsNaN(d))
            {
                stream.WriteByte(HproseTags.TagNaN);
            }
            else if (double.IsInfinity(d))
            {
                stream.WriteByte(HproseTags.TagInfinity);
                if (d > 0)
                {
                    stream.WriteByte(HproseTags.TagPos);
                }
                else
                {
                    stream.WriteByte(HproseTags.TagNeg);
                }
            }
            else
            {
                stream.WriteByte(HproseTags.TagDouble);
                WriteAsciiString(d.ToString("R"));
                stream.WriteByte(HproseTags.TagSemicolon);
            }
        }

        public void WriteDouble(decimal d)
        {
            stream.WriteByte(HproseTags.TagDouble);
            WriteAsciiString(d.ToString());
            stream.WriteByte(HproseTags.TagSemicolon);
        }

        public void WriteNaN()
        {
            stream.WriteByte(HproseTags.TagNaN);
        }

        public void WriteInfinity(bool positive)
        {
            stream.WriteByte(HproseTags.TagInfinity);
            if (positive)
            {
                stream.WriteByte(HproseTags.TagPos);
            }
            else
            {
                stream.WriteByte(HproseTags.TagNeg);
            }
        }

        public void WriteNull()
        {
            stream.WriteByte(HproseTags.TagNull);
        }

        public void WriteEmpty()
        {
            stream.WriteByte(HproseTags.TagEmpty);
        }

        public void WriteBoolean(bool b)
        {
            if (b)
            {
                stream.WriteByte(HproseTags.TagTrue);
            }
            else
            {
                stream.WriteByte(HproseTags.TagFalse);
            }
        }

        private void WriteDate(int year, int month, int day)
        {
            stream.WriteByte(HproseTags.TagDate);
            stream.WriteByte((byte)('0' + (year / 1000 % 10)));
            stream.WriteByte((byte)('0' + (year / 100 % 10)));
            stream.WriteByte((byte)('0' + (year / 10 % 10)));
            stream.WriteByte((byte)('0' + (year % 10)));
            stream.WriteByte((byte)('0' + (month / 10 % 10)));
            stream.WriteByte((byte)('0' + (month % 10)));
            stream.WriteByte((byte)('0' + (day / 10 % 10)));
            stream.WriteByte((byte)('0' + (day % 10)));
        }

        private void WriteTime(int hour, int minute, int second, int millisecond)
        {
            stream.WriteByte(HproseTags.TagTime);
            stream.WriteByte((byte)('0' + (hour / 10 % 10)));
            stream.WriteByte((byte)('0' + (hour % 10)));
            stream.WriteByte((byte)('0' + (minute / 10 % 10)));
            stream.WriteByte((byte)('0' + (minute % 10)));
            stream.WriteByte((byte)('0' + (second / 10 % 10)));
            stream.WriteByte((byte)('0' + (second % 10)));
            if (millisecond > 0)
            {
                stream.WriteByte(HproseTags.TagPoint);
                stream.WriteByte((byte)('0' + (millisecond / 100 % 10)));
                stream.WriteByte((byte)('0' + (millisecond / 10 % 10)));
                stream.WriteByte((byte)('0' + (millisecond % 10)));
            }
        }

        private void WriteDateTime(DateTime datetime)
        {
            int year = datetime.Year;
            int month = datetime.Month;
            int day = datetime.Day;
            int hour = datetime.Hour;
            int minute = datetime.Minute;
            int second = datetime.Second;
            int millisecond = datetime.Millisecond;
            byte tag = HproseTags.TagSemicolon;
            if (datetime.Kind == DateTimeKind.Utc) tag = HproseTags.TagUTC;
            if ((hour == 0) && (minute == 0) && (second == 0) && (millisecond == 0))
            {
                WriteDate(year, month, day);
                stream.WriteByte(tag);
            }
            else if ((year == 1970) && (month == 1) && (day == 1))
            {
                WriteTime(hour, minute, second, millisecond);
                stream.WriteByte(tag);
            }
            else
            {
                WriteDate(year, month, day);
                WriteTime(hour, minute, second, millisecond);
                stream.WriteByte(tag);
            }
        }

        public void WriteDate(DateTime date)
        {
            refer.Set(date);
            WriteDateTime(date);
        }

        public void WriteDateWithRef(DateTime date)
        {
            if (!refer.Write(date)) WriteDate(date);
        }

        public void WriteBytes(byte[] bytes)
        {
            refer.Set(bytes);
            stream.WriteByte(HproseTags.TagBytes);
            if (bytes.Length > 0) WriteInt(bytes.Length, stream);
            stream.WriteByte(HproseTags.TagQuote);
            stream.Write(bytes, 0, bytes.Length);
            stream.WriteByte(HproseTags.TagQuote);
        }

        public void WriteBytesWithRef(byte[] bytes)
        {
            if (!refer.Write(bytes)) WriteBytes(bytes);
        }

        public void WriteStream(Stream s)
        {
            if (!s.CanRead) throw new HproseException("This stream can't support serialize.");
            refer.Set(s);
            stream.WriteByte(HproseTags.TagBytes);
            long oldPos = 0;
            if (s.CanSeek)
            {
                oldPos = s.Position;
                s.Position = 0;
            }
            int length = (int)s.Length;
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagQuote);
            byte[] buffer = new byte[4096];
            while ((length = s.Read(buffer, 0, 4096)) != 0)
            {
                stream.Write(buffer, 0, length);
            }
            stream.WriteByte(HproseTags.TagQuote);
            if (s.CanSeek)
            {
                s.Position = oldPos;
            }
        }

        public void WriteStreamWithRef(Stream s)
        {
            if (!refer.Write(s)) WriteStream(s);
        }

        public void WriteUTF8Char(int c)
        {
            stream.WriteByte(HproseTags.TagUTF8Char);
            if (c < 0x80)
            {
                stream.WriteByte((byte)c);
            }
            else if (c < 0x800)
            {
                stream.WriteByte((byte)(0xc0 | (c >> 6)));
                stream.WriteByte((byte)(0x80 | (c & 0x3f)));
            }
            else
            {
                stream.WriteByte((byte)(0xe0 | (c >> 12)));
                stream.WriteByte((byte)(0x80 | ((c >> 6) & 0x3f)));
                stream.WriteByte((byte)(0x80 | (c & 0x3f)));
            }
        }

        public void WriteString(string s)
        {
            refer.Set(s);
            stream.WriteByte(HproseTags.TagString);
            WriteUTF8String(s, stream);
        }

        public void WriteStringWithRef(string s)
        {
            if (!refer.Write(s)) WriteString(s);
        }

        private void WriteUTF8String(string s, Stream stream)
        {
            int length = s.Length;
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagQuote);
            for (int i = 0; i < length; ++i)
            {
                int c = 0xffff & s[i];
                if (c < 0x80)
                {
                    stream.WriteByte((byte)c);
                }
                else if (c < 0x800)
                {
                    stream.WriteByte((byte)(0xc0 | (c >> 6)));
                    stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                }
                else if (c < 0xd800 || c > 0xdfff)
                {
                    stream.WriteByte((byte)(0xe0 | (c >> 12)));
                    stream.WriteByte((byte)(0x80 | ((c >> 6) & 0x3f)));
                    stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                }
                else
                {
                    if (++i < length)
                    {
                        int c2 = 0xffff & s[i];
                        if (c < 0xdc00 && 0xdc00 <= c2 && c2 <= 0xdfff)
                        {
                            c = ((c & 0x03ff) << 10 | (c2 & 0x03ff)) + 0x010000;
                            stream.WriteByte((byte)(0xf0 | (c >> 18)));
                            stream.WriteByte((byte)(0x80 | ((c >> 12) & 0x3f)));
                            stream.WriteByte((byte)(0x80 | ((c >> 6) & 0x3f)));
                            stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                        }
                        else
                        {
                            throw new HproseException("wrong unicode string");
                        }
                    }
                    else
                    {
                        throw new HproseException("wrong unicode string");
                    }
                }
            }
            stream.WriteByte(HproseTags.TagQuote);
        }

        public void WriteString(char[] s)
        {
            refer.Set(s);
            stream.WriteByte(HproseTags.TagString);
            WriteUTF8String(s);
        }

        public void WriteStringWithRef(char[] s)
        {
            if (!refer.Write(s)) WriteString(s);
        }

        private void WriteUTF8String(char[] s)
        {
            int length = s.Length;
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagQuote);
            for (int i = 0; i < length; ++i)
            {
                int c = 0xffff & s[i];
                if (c < 0x80)
                {
                    stream.WriteByte((byte)c);
                }
                else if (c < 0x800)
                {
                    stream.WriteByte((byte)(0xc0 | (c >> 6)));
                    stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                }
                else if (c < 0xd800 || c > 0xdfff)
                {
                    stream.WriteByte((byte)(0xe0 | (c >> 12)));
                    stream.WriteByte((byte)(0x80 | ((c >> 6) & 0x3f)));
                    stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                }
                else
                {
                    if (++i < length)
                    {
                        int c2 = 0xffff & s[i];
                        if (c < 0xdc00 && 0xdc00 <= c2 && c2 <= 0xdfff)
                        {
                            c = ((c & 0x03ff) << 10 | (c2 & 0x03ff)) + 0x010000;
                            stream.WriteByte((byte)(0xf0 | (c >> 18)));
                            stream.WriteByte((byte)(0x80 | ((c >> 12) & 0x3f)));
                            stream.WriteByte((byte)(0x80 | ((c >> 6) & 0x3f)));
                            stream.WriteByte((byte)(0x80 | (c & 0x3f)));
                        }
                        else
                        {
                            throw new HproseException("wrong unicode string");
                        }
                    }
                    else
                    {
                        throw new HproseException("wrong unicode string");
                    }
                }
            }
            stream.WriteByte(HproseTags.TagQuote);
        }

        public void WriteString(StringBuilder s)
        {
            refer.Set(s);
            stream.WriteByte(HproseTags.TagString);
            WriteUTF8String(s.ToString(), stream);
        }

        public void WriteStringWithRef(StringBuilder s)
        {
            if (!refer.Write(s)) WriteString(s);
        }

        public void WriteGuid(Guid g)
        {
            refer.Set(g);
            stream.WriteByte(HproseTags.TagGuid);
            stream.WriteByte(HproseTags.TagOpenbrace);
            WriteAsciiString(g.ToString());
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteGuidWithRef(Guid g)
        {
            if (!refer.Write(g)) WriteGuid(g);
        }


        public void WriteArray(sbyte[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteInteger(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(sbyte[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(short[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteInteger(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(short[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(int[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteInteger(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(int[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(long[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteLong(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(long[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(ushort[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteInteger(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(ushort[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(uint[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteLong(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(uint[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(ulong[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteLong(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(ulong[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(IntPtr[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteIntPtr(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(IntPtr[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(BigInteger[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteLong(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(BigInteger[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(float[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteDouble(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(float[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(double[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteDouble(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(double[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(decimal[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteDouble(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(decimal[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(bool[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteBoolean(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(bool[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(byte[][] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                byte[] value = array[i];
                if (value == null) WriteNull();
                else WriteBytes(value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(byte[][] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(char[][] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                char[] value = array[i];
                if (value == null) WriteNull();
                else WriteString(value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(char[][] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(string[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                string value = array[i];
                if (value == null) WriteNull();
                else WriteString(value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(string[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(StringBuilder[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                StringBuilder value = array[i];
                if (value == null) WriteNull();
                else WriteString(value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(StringBuilder[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(Guid[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteGuidWithRef(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(Guid[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(TimeSpan[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteLong(array[i].Ticks);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(TimeSpan[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }


        public void WriteArray(DateTime[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteDate(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }


        public void WriteArrayWithRef(DateTime[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }

        public void WriteArray(object[] array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                Serialize(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteArrayWithRef(object[] array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }

        public void WriteArray(Array array)
        {
            refer.Set(array);
            int rank = array.Rank;
            if (rank == 1)
            {
                int length = array.Length;
                stream.WriteByte(HproseTags.TagList);
                if (length > 0) WriteInt(length, stream);
                stream.WriteByte(HproseTags.TagOpenbrace);
                for (int i = 0; i < length; ++i)
                {
                    Serialize(array.GetValue(i));
                }
                stream.WriteByte(HproseTags.TagClosebrace);
            }
            else
            {
                int i;
                int[,] des = new int[rank, 2];
                int[] loc = new int[rank];
                int[] len = new int[rank];
                int maxrank = rank - 1;
                for (i = 0; i < rank; ++i)
                {
                    des[i, 0] = array.GetLowerBound(i);
                    des[i, 1] = array.GetUpperBound(i);
                    loc[i] = des[i, 0];
                    len[i] = array.GetLength(i);
                }
                stream.WriteByte(HproseTags.TagList);
                if (len[0] > 0) WriteInt(len[0], stream);
                stream.WriteByte(HproseTags.TagOpenbrace);
                while (loc[0] <= des[0, 1])
                {
                    int n = 0;
                    for (i = maxrank; i > 0; i--)
                    {
                        if (loc[i] == des[i, 0])
                        {
                            n++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    for (i = rank - n; i < rank; ++i)
                    {
                        refer.Set(new object());
                        stream.WriteByte(HproseTags.TagList);
                        if (len[i] > 0) WriteInt(len[i], stream);
                        stream.WriteByte(HproseTags.TagOpenbrace);
                    }
                    for (loc[maxrank] = des[maxrank, 0];
                         loc[maxrank] <= des[maxrank, 1];
                         loc[maxrank]++)
                    {
                        Serialize(array.GetValue(loc));
                    }
                    for (i = maxrank; i > 0; i--)
                    {
                        if (loc[i] > des[i, 1])
                        {
                            loc[i] = des[i, 0];
                            loc[i - 1]++;
                            stream.WriteByte(HproseTags.TagClosebrace);
                        }
                    }
                }
                stream.WriteByte(HproseTags.TagClosebrace);
            }
        }

        public void WriteArrayWithRef(Array array)
        {
            if (!refer.Write(array)) WriteArray(array);
        }

        public void WriteBitArray(BitArray array)
        {
            refer.Set(array);
            int length = array.Length;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteBoolean(array[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteBitArrayWithRef(BitArray array)
        {
            if (!refer.Write(array)) WriteBitArray(array);
        }

        public void WriteList(IList list)
        {
            refer.Set(list);
            int length = list.Count;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                Serialize(list[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteListWithRef(IList list)
        {
            if (!refer.Write(list)) WriteList(list);
        }

        public void WriteMap(IDictionary map)
        {
            refer.Set(map);
            int length = map.Count;
            stream.WriteByte(HproseTags.TagMap);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            foreach (DictionaryEntry e in map)
            {
                Serialize(e.Key);
                Serialize(e.Value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteMapWithRef(IDictionary map)
        {
            if (!refer.Write(map)) WriteMap(map);
        }

        public void WriteCollection(ICollection collection)
        {
            refer.Set(collection);
            int length = collection.Count;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            foreach (object e in collection)
            {
                Serialize(e);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteCollectionWithRef(ICollection collection)
        {
            if (!refer.Write(collection)) WriteCollection(collection);
        }

        public void WriteList(IList<double> list)
        {
            refer.Set(list);
            int length = list.Count;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteDouble(list[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteListWithRef(IList<double> list)
        {
            if (!refer.Write(list)) WriteList(list);
        }

        public void WriteList(IList<int> list)
        {
            refer.Set(list);
            int length = list.Count;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                WriteInteger(list[i]);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteListWithRef(IList<int> list)
        {
            if (!refer.Write(list)) WriteList(list);
        }

        public void WriteList(IList<string> list)
        {
            refer.Set(list);
            int length = list.Count;
            stream.WriteByte(HproseTags.TagList);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            for (int i = 0; i < length; ++i)
            {
                string value = list[i];
                if (value == null) WriteNull();
                else WriteStringWithRef(value);
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteListWithRef(IList<string> list)
        {
            if (!refer.Write(list)) WriteList(list);
        }

        public void WriteAnonymousType(object obj)
        {
            refer.Set(obj);

            PropertyInfo[] properties = obj.GetType().GetProperties();
            int length = properties.Length;
            stream.WriteByte(HproseTags.TagMap);
            if (length > 0) WriteInt(length, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            foreach (PropertyInfo property in properties)
            {
                WriteString(property.Name);
                Serialize(property.GetValue(obj, null));
            }
            stream.WriteByte(HproseTags.TagClosebrace);
        }

        public void WriteAnonymousTypeWithRef(object obj)
        {
            if (!refer.Write(obj)) WriteAnonymousType(obj);
        }

        public void WriteObject(object obj)
        {
            Type type = obj.GetType();
            int cr;

            if (!classref.TryGetValue(type, out cr))
            {
                cr = WriteClass(type);
            }
            refer.Set(obj);
            stream.WriteByte(HproseTags.TagObject);
            WriteInt(cr, stream);
            stream.WriteByte(HproseTags.TagOpenbrace);
            if ((mode != HproseMode.MemberMode) && HproseHelper.IsSerializable(type))
            {
                WriteSerializableObject(obj, type);
            }
            else
            {
                WriteDataContractObject(obj, type);
            }

            stream.WriteByte(HproseTags.TagClosebrace);
        }
        public void WriteObjectWithRef(object obj)
        {
            if (!refer.Write(obj)) WriteObject(obj);
        }

        private void WriteSerializableObject(object obj, Type type)
        {
            if (mode == HproseMode.FieldMode)
            {
                ObjectSerializer.Get(type).SerializeFields(obj, this);

            }
            else
            {
                ObjectSerializer.Get(type).SerializeProperties(obj, this);
            }
        }

        private void WriteDataContractObject(object obj, Type type)
        {
            ObjectSerializer.Get(type).SerializeMembers(obj, this);

        }
        private int WriteClass(Type type)
        {
            SerializeCache cache = null;
            if ((mode != HproseMode.MemberMode) && HproseHelper.IsSerializable(type))
            {
                cache = WriteSerializableClass(type);
            }
            else
            {
                cache = WriteDataContractClass(type);
            }

            stream.Write(cache.data, 0, cache.data.Length);
            refer.AddCount(cache.refcount);
            int cr = lastclassref++;
            classref[type] = cr;
            return cr;
        }

        private SerializeCache WriteSerializableClass(Type type)
        {
            SerializeCache cache = null;
            ICollection c = null;
            if (mode == HproseMode.FieldMode)
            {
                c = fieldsCache;
                lock (c.SyncRoot)
                {
                    fieldsCache.TryGetValue(type, out cache);

                }
            }
            else
            {
                c = propertiesCache;
                lock (c.SyncRoot)
                {
                    propertiesCache.TryGetValue(type, out cache);

                }
            }
            if (cache == null)
            {
                cache = new SerializeCache();
                MemoryStream cachestream = new MemoryStream();
                ICollection<string> keys;

                if (mode == HproseMode.FieldMode)
                {
                    keys = HproseHelper.GetFields(type).Keys;
                }
                else
                {
                    keys = HproseHelper.GetProperties(type).Keys;
                }

                int count = keys.Count;
                cachestream.WriteByte(HproseTags.TagClass);
                WriteUTF8String(HproseHelper.GetClassName(type), cachestream);
                if (count > 0) WriteInt(count, cachestream);
                cachestream.WriteByte(HproseTags.TagOpenbrace);
                foreach (string key in keys)
                {
                    cachestream.WriteByte(HproseTags.TagString);
                    WriteUTF8String(key, cachestream);

                    cache.refcount++;
                }
                cachestream.WriteByte(HproseTags.TagClosebrace);
                cache.data = cachestream.ToArray();
                if (mode == HproseMode.FieldMode)
                {
                    c = fieldsCache;
                    lock (c.SyncRoot)
                    {
                        fieldsCache[type] = cache;
                    }
                }
                else
                {
                    c = propertiesCache;
                    lock (c)
                    {
                        propertiesCache[type] = cache;
                    }
                }
            }
            return cache;
        }

        private SerializeCache WriteDataContractClass(Type type)
        {
            SerializeCache cache = null;
            ICollection c = membersCache;
            lock (c.SyncRoot)
            {
                membersCache.TryGetValue(type, out cache);

            }
            if (cache == null)
            {
                cache = new SerializeCache();
                MemoryStream cachestream = new MemoryStream();
                ICollection<string> keys;

                keys = HproseHelper.GetMembers(type).Keys;
                int count = keys.Count;
                cachestream.WriteByte(HproseTags.TagClass);
                WriteUTF8String(HproseHelper.GetClassName(type), cachestream);
                if (count > 0) WriteInt(count, cachestream);
                cachestream.WriteByte(HproseTags.TagOpenbrace);
                foreach (string key in keys)
                {
                    cachestream.WriteByte(HproseTags.TagString);
                    WriteUTF8String(key, cachestream);

                    cache.refcount++;
                }
                cachestream.WriteByte(HproseTags.TagClosebrace);
                cache.data = cachestream.ToArray();
                lock (c.SyncRoot)
                {
                    membersCache[type] = cache;
                }
            }
            return cache;
        }

        private void WriteAsciiString(string s)
        {
            int size = s.Length;
            byte[] b = new byte[size--];
            for (; size >= 0; size--)
            {
                b[size] = (byte)s[size];
            }
            stream.Write(b, 0, b.Length);
        }

        private void WriteIntFast(int i, Stream stream)
        {
            int off = 20;
            int len = 0;
            bool neg = false;
            if (i < 0)
            {
                neg = true;
                i = -i;
            }
            while (i != 0)
            {
                buf[--off] = (byte)(i % 10 + (byte)'0');
                ++len;
                i /= 10;
            }
            if (neg)
            {
                buf[--off] = (byte)'-';
                ++len;
            }
            stream.Write(buf, off, len);
        }

        private void WriteIntFast(uint i, Stream stream)
        {
            int off = 20;
            int len = 0;
            while (i != 0)
            {
                buf[--off] = (byte)(i % 10 + (byte)'0');
                ++len;
                i /= 10;
            }
            stream.Write(buf, off, len);
        }

        private void WriteIntFast(long i, Stream stream)
        {
            int off = 20;
            int len = 0;
            bool neg = false;
            if (i < 0)
            {
                neg = true;
                i = -i;
            }
            while (i != 0)
            {
                buf[--off] = (byte)(i % 10 + (byte)'0');
                ++len;
                i /= 10;
            }
            if (neg)
            {
                buf[--off] = (byte)'-';
                ++len;
            }
            stream.Write(buf, off, len);
        }

        private void WriteIntFast(ulong i, Stream stream)
        {
            int off = 20;
            int len = 0;
            while (i != 0)
            {
                buf[--off] = (byte)(i % 10 + (byte)'0');
                ++len;
                i /= 10;
            }
            stream.Write(buf, off, len);
        }

        internal void WriteInt(int i, Stream stream)
        {
            if (i >= 0 && i <= 9)
            {
                stream.WriteByte((byte)('0' + i));
            }
            else
            {
                WriteIntFast((uint)i, stream);
            }
        }

        public void Reset()
        {
            refer.Reset();
            classref.Clear();
            lastclassref = 0;
        }

        private class SerializeCache
        {
            public byte[] data;
            public int refcount;
        }
    }
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
    internal sealed class IgnoreDataMemberAttribute : Attribute
    {
        public IgnoreDataMemberAttribute()
        {
        }
    }
    internal class ObjectSerializer
    {
        private delegate void SerializeDelegate(object value, HproseWriter writer);
        private static readonly Type typeofSerializeDelegate = typeof(SerializeDelegate);
        private static readonly Type typeofVoid = typeof(void);
        private static readonly Type typeofObject = typeof(object);
        private static readonly Type[] typeofArgs = new Type[] { typeofObject, typeof(HproseWriter) };
        private static readonly Type typeofException = typeof(Exception);
        private static readonly MethodInfo serializeMethod = typeof(HproseWriter).GetMethod("Serialize", new Type[] { typeofObject });
        private static readonly ConstructorInfo hproseExceptionCtor = typeof(HproseException).GetConstructor(new Type[] { typeof(string), typeofException });
        private SerializeDelegate serializeFieldsDelegate;
        private SerializeDelegate serializePropertiesDelegate;
        private SerializeDelegate serializeMembersDelegate;

        private static readonly ReaderWriterLockSlim serializersCacheLock = new ReaderWriterLockSlim();

        private static readonly Dictionary<Type, ObjectSerializer> serializersCache = new Dictionary<Type, ObjectSerializer>();

        private void InitSerializeFieldsDelegate(Type type)
        {
            ICollection<FieldTypeInfo> fields = HproseHelper.GetFields(type).Values;
            DynamicMethod dynamicMethod = new DynamicMethod("$SerializeFields",
                typeofVoid,
                typeofArgs,
                type,
                true);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder value = gen.DeclareLocal(typeofObject);
            LocalBuilder e = gen.DeclareLocal(typeofException);
            foreach (FieldTypeInfo field in fields)
            {
                Label exTryCatch = gen.BeginExceptionBlock();
                if (type.IsValueType)
                {
                    gen.Emit(OpCodes.Ldarg_0);
                    gen.Emit(OpCodes.Unbox, type);
                }
                else
                {
                    gen.Emit(OpCodes.Ldarg_0);
                }
                gen.Emit(OpCodes.Ldfld, field.info);
                if (field.type.IsValueType)
                {
                    gen.Emit(OpCodes.Box, field.type);
                }
                gen.Emit(OpCodes.Stloc_S, value);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.BeginCatchBlock(typeofException);
                gen.Emit(OpCodes.Stloc_S, e);
                gen.Emit(OpCodes.Ldstr, "The field value can\'t be serialized.");
                gen.Emit(OpCodes.Ldloc_S, e);
                gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                gen.Emit(OpCodes.Throw);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.EndExceptionBlock();
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Ldloc_S, value);
                gen.Emit(OpCodes.Call, serializeMethod);
            }
            gen.Emit(OpCodes.Ret);
            serializeFieldsDelegate = (SerializeDelegate)dynamicMethod.CreateDelegate(typeofSerializeDelegate);
        }

        private void InitSerializePropertiesDelegate(Type type)
        {
            ICollection<PropertyTypeInfo> properties = HproseHelper.GetProperties(type).Values;
            DynamicMethod dynamicMethod = new DynamicMethod("$SerializeProperties",
                typeofVoid,
                typeofArgs,
                type,
                true);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder value = gen.DeclareLocal(typeofObject);
            LocalBuilder e = gen.DeclareLocal(typeofException);
            foreach (PropertyTypeInfo property in properties)
            {
                Label exTryCatch = gen.BeginExceptionBlock();
                if (type.IsValueType)
                {
                    gen.Emit(OpCodes.Ldarg_0);
                    gen.Emit(OpCodes.Unbox, type);
                }
                else
                {
                    gen.Emit(OpCodes.Ldarg_0);
                }
                MethodInfo getMethod = property.info.GetGetMethod(true);
                if (getMethod.IsVirtual)
                {
                    gen.Emit(OpCodes.Callvirt, getMethod);
                }
                else
                {
                    gen.Emit(OpCodes.Call, getMethod);
                }
                if (property.type.IsValueType)
                {
                    gen.Emit(OpCodes.Box, property.type);
                }
                gen.Emit(OpCodes.Stloc_S, value);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.BeginCatchBlock(typeofException);
                gen.Emit(OpCodes.Stloc_S, e);
                gen.Emit(OpCodes.Ldstr, "The property value can\'t be serialized.");
                gen.Emit(OpCodes.Ldloc_S, e);
                gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                gen.Emit(OpCodes.Throw);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.EndExceptionBlock();
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Ldloc_S, value);
                gen.Emit(OpCodes.Call, serializeMethod);
            }
            gen.Emit(OpCodes.Ret);
            serializePropertiesDelegate = (SerializeDelegate)dynamicMethod.CreateDelegate(typeofSerializeDelegate);
        }

        private void InitSerializeMembersDelegate(Type type)
        {
            ICollection<MemberTypeInfo> members = HproseHelper.GetMembers(type).Values;
            DynamicMethod dynamicMethod = new DynamicMethod("$SerializeFields",
                typeofVoid,
                typeofArgs,
                type,
                true);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder value = gen.DeclareLocal(typeofObject);
            LocalBuilder e = gen.DeclareLocal(typeofException);
            foreach (MemberTypeInfo member in members)
            {
                Label exTryCatch = gen.BeginExceptionBlock();
                if (type.IsValueType)
                {
                    gen.Emit(OpCodes.Ldarg_0);
                    gen.Emit(OpCodes.Unbox, type);
                }
                else
                {
                    gen.Emit(OpCodes.Ldarg_0);
                }
                if (member.info is FieldInfo)
                {
                    FieldInfo fieldInfo = (FieldInfo)member.info;
                    gen.Emit(OpCodes.Ldfld, fieldInfo);
                    if (member.type.IsValueType)
                    {
                        gen.Emit(OpCodes.Box, member.type);
                    }
                }
                else
                {
                    PropertyInfo propertyInfo = (PropertyInfo)member.info;
                    MethodInfo getMethod = propertyInfo.GetGetMethod(true);
                    if (getMethod.IsVirtual)
                    {
                        gen.Emit(OpCodes.Callvirt, getMethod);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Call, getMethod);
                    }
                    if (member.type.IsValueType)
                    {
                        gen.Emit(OpCodes.Box, member.type);
                    }
                }
                gen.Emit(OpCodes.Stloc_S, value);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.BeginCatchBlock(typeofException);
                gen.Emit(OpCodes.Stloc_S, e);
                gen.Emit(OpCodes.Ldstr, "The member value can\'t be serialized.");
                gen.Emit(OpCodes.Ldloc_S, e);
                gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                gen.Emit(OpCodes.Throw);
                gen.Emit(OpCodes.Leave_S, exTryCatch);
                gen.EndExceptionBlock();
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Ldloc_S, value);
                gen.Emit(OpCodes.Call, serializeMethod);
            }
            gen.Emit(OpCodes.Ret);
            serializeMembersDelegate = (SerializeDelegate)dynamicMethod.CreateDelegate(typeofSerializeDelegate);
        }

        private ObjectSerializer(Type type)
        {
            InitSerializeFieldsDelegate(type);
            InitSerializePropertiesDelegate(type);
            InitSerializeMembersDelegate(type);
        }

        public static ObjectSerializer Get(Type type)
        {
            ObjectSerializer serializer = null;
            try
            {
                serializersCacheLock.EnterReadLock();

                if (serializersCache.TryGetValue(type, out serializer))
                {
                    return serializer;
                }
            }
            finally
            {
                serializersCacheLock.ExitReadLock();

            }
            try
            {
                serializersCacheLock.EnterWriteLock();

                if (serializersCache.TryGetValue(type, out serializer))
                {
                    return serializer;
                }
                serializer = new ObjectSerializer(type);
                serializersCache[type] = serializer;
            }
            finally
            {
                serializersCacheLock.ExitWriteLock();

            }
            return serializer;
        }

        public void SerializeFields(object value, HproseWriter writer)
        {
            serializeFieldsDelegate(value, writer);
        }

        public void SerializeProperties(object value, HproseWriter writer)
        {
            serializePropertiesDelegate(value, writer);
        }

        public void SerializeMembers(object value, HproseWriter writer)
        {
            serializeMembersDelegate(value, writer);
        }
    }
    internal abstract class ObjectUnserializer
    {
        protected delegate ObjectUnserializer CreateObjectUnserializerDelegate(Type type, string[] names);
        protected static readonly Type typeofException = typeof(Exception);
        protected static ConstructorInfo hproseExceptionCtor = typeof(HproseException).GetConstructor(new Type[] { typeof(string), typeofException });
        private static readonly Type typeofVoid = typeof(void);
        private static readonly Type typeofObject = typeof(object);
        private static readonly Type[] typeofArgs = new Type[] { typeofObject, typeof(object[]) };
        private delegate void UnserializeDelegate(object result, object[] values);
        private static readonly Type typeofUnserializeDelegate = typeof(UnserializeDelegate);
        private UnserializeDelegate unserializeDelegate;
        private static readonly ReaderWriterLockSlim unserializersCacheLock = new ReaderWriterLockSlim();

        private static readonly Dictionary<CacheKey, ObjectUnserializer> unserializersCache = new Dictionary<CacheKey, ObjectUnserializer>();

        public ObjectUnserializer(Type type, string[] names)
        {
            DynamicMethod dynamicMethod = new DynamicMethod("$Unserialize",
                typeofVoid,
                typeofArgs,
                type,
                true);
            InitUnserializeDelegate(type, names, dynamicMethod);
            unserializeDelegate = (UnserializeDelegate)dynamicMethod.CreateDelegate(typeofUnserializeDelegate);
        }

        protected abstract void InitUnserializeDelegate(Type type, string[] names, DynamicMethod dynamicMethod);

        protected static ObjectUnserializer Get(HproseMode mode, Type type, string[] names, CreateObjectUnserializerDelegate createObjectUnserializer)
        {
            CacheKey key = new CacheKey(mode, type, names);
            ObjectUnserializer unserializer = null;
            try
            {
                unserializersCacheLock.EnterReadLock();

                if (unserializersCache.TryGetValue(key, out unserializer))
                {
                    return unserializer;
                }
            }
            finally
            {
                unserializersCacheLock.ExitReadLock();

            }
            try
            {
                unserializersCacheLock.EnterWriteLock();

                if (unserializersCache.TryGetValue(key, out unserializer))
                {
                    return unserializer;
                }
                unserializer = createObjectUnserializer(type, names);
                unserializersCache[key] = unserializer;
            }
            finally
            {
                unserializersCacheLock.ExitWriteLock();

            }
            return unserializer;
        }

        public void Unserialize(object obj, object[] values)
        {
            unserializeDelegate(obj, values);
        }

        private class CacheKey
        {
            private HproseMode mode;
            private Type type;
            private string[] names;
            private int hash;

            public CacheKey(HproseMode mode, Type type, string[] names)
            {
                this.mode = mode;
                this.type = type;
                this.names = names;
            }

            public static bool operator ==(CacheKey k1, CacheKey k2)
            {
                if (!k1.mode.Equals(k2.mode))
                    return false;
                if (!k1.type.Equals(k2.type))
                    return false;
                if (k1.names.Length != k2.names.Length)
                    return false;
                for (int i = 0; i < k1.names.Length; ++i)
                    if (!k1.names[i].Equals(k2.names[i]))
                        return false;
                return true;
            }

            public static bool operator !=(CacheKey k1, CacheKey k2)
            {
                return !(k1 == k2);
            }

            public override bool Equals(object obj)
            {
                if (!(obj is CacheKey))
                    return false;

                return this == (CacheKey)obj;
            }

            public override int GetHashCode()
            {
                if (hash == 0)
                {
                    hash = mode.GetHashCode() * 31 + type.GetHashCode();
                    foreach (string name in names)
                        hash = hash * 31 + name.GetHashCode();
                }
                return hash;
            }
        }
    }

    internal class ObjectFieldModeUnserializer : ObjectUnserializer
    {

        public ObjectFieldModeUnserializer(Type type, string[] names)
            : base(type, names)
        {
        }

        protected override void InitUnserializeDelegate(Type type, string[] names, DynamicMethod dynamicMethod)
        {
            Dictionary<string, FieldTypeInfo> fields = HproseHelper.GetFields(type);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder e = gen.DeclareLocal(typeofException);
            int count = names.Length;
            for (int i = 0; i < count; ++i)
            {
                FieldTypeInfo field;
                if (fields.TryGetValue(names[i], out field))
                {
                    Label exTryCatch = gen.BeginExceptionBlock();
                    if (type.IsValueType)
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                        gen.Emit(OpCodes.Unbox, type);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                    }
                    gen.Emit(OpCodes.Ldarg_1);
                    gen.Emit(OpCodes.Ldc_I4, i);
                    gen.Emit(OpCodes.Ldelem_Ref);
                    if (field.type.IsValueType)
                    {
                        gen.Emit(OpCodes.Unbox_Any, field.type);
                    }
                    gen.Emit(OpCodes.Stfld, field.info);
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.BeginCatchBlock(typeofException);
                    gen.Emit(OpCodes.Stloc_S, e);
                    gen.Emit(OpCodes.Ldstr, "The field value can\'t be unserialized.");
                    gen.Emit(OpCodes.Ldloc_S, e);
                    gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                    gen.Emit(OpCodes.Throw);
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.EndExceptionBlock();
                }
            }
            gen.Emit(OpCodes.Ret);
        }

        private static ObjectUnserializer CreateObjectUnserializer(Type type, string[] names)
        {
            return new ObjectFieldModeUnserializer(type, names);
        }
        public static ObjectUnserializer Get(Type type, string[] names)
        {
            return ObjectUnserializer.Get(HproseMode.FieldMode, type, names, CreateObjectUnserializer);
        }
    }

    internal class ObjectPropertyModeUnserializer : ObjectUnserializer
    {

        public ObjectPropertyModeUnserializer(Type type, string[] names)
            : base(type, names)
        {
        }

        protected override void InitUnserializeDelegate(Type type, string[] names, DynamicMethod dynamicMethod)
        {
            Dictionary<string, PropertyTypeInfo> properties = HproseHelper.GetProperties(type);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder e = gen.DeclareLocal(typeofException);
            int count = names.Length;
            for (int i = 0; i < count; ++i)
            {
                PropertyTypeInfo property;
                if (properties.TryGetValue(names[i], out property))
                {
                    Label exTryCatch = gen.BeginExceptionBlock();
                    if (type.IsValueType)
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                        gen.Emit(OpCodes.Unbox, type);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                    }
                    gen.Emit(OpCodes.Ldarg_1);
                    gen.Emit(OpCodes.Ldc_I4, i);
                    gen.Emit(OpCodes.Ldelem_Ref);
                    if (property.type.IsValueType)
                    {
                        gen.Emit(OpCodes.Unbox_Any, property.type);
                    }
                    MethodInfo setMethod = property.info.GetSetMethod(true);
                    if (setMethod.IsVirtual)
                    {
                        gen.Emit(OpCodes.Callvirt, setMethod);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Call, setMethod);
                    }
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.BeginCatchBlock(typeofException);
                    gen.Emit(OpCodes.Stloc_S, e);
                    gen.Emit(OpCodes.Ldstr, "The property value can\'t be unserialized.");
                    gen.Emit(OpCodes.Ldloc_S, e);
                    gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                    gen.Emit(OpCodes.Throw);
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.EndExceptionBlock();
                }
            }
            gen.Emit(OpCodes.Ret);
        }

        private static ObjectUnserializer CreateObjectUnserializer(Type type, string[] names)
        {
            return new ObjectPropertyModeUnserializer(type, names);
        }
        public static ObjectUnserializer Get(Type type, string[] names)
        {
            return ObjectUnserializer.Get(HproseMode.PropertyMode, type, names, CreateObjectUnserializer);
        }
    }

    internal class ObjectMemberModeUnserializer : ObjectUnserializer
    {

        public ObjectMemberModeUnserializer(Type type, string[] names)
            : base(type, names)
        {
        }

        protected override void InitUnserializeDelegate(Type type, string[] names, DynamicMethod dynamicMethod)
        {
            Dictionary<string, MemberTypeInfo> members = HproseHelper.GetMembers(type);
            ILGenerator gen = dynamicMethod.GetILGenerator();
            LocalBuilder e = gen.DeclareLocal(typeofException);
            int count = names.Length;
            for (int i = 0; i < count; ++i)
            {
                MemberTypeInfo member;
                if (members.TryGetValue(names[i], out member))
                {
                    Label exTryCatch = gen.BeginExceptionBlock();
                    if (type.IsValueType)
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                        gen.Emit(OpCodes.Unbox, type);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldarg_0);
                    }
                    gen.Emit(OpCodes.Ldarg_1);
                    gen.Emit(OpCodes.Ldc_I4, i);
                    gen.Emit(OpCodes.Ldelem_Ref);
                    if (member.info is FieldInfo)
                    {
                        FieldInfo fieldInfo = (FieldInfo)member.info;
                        if (member.type.IsValueType)
                        {
                            gen.Emit(OpCodes.Unbox_Any, member.type);
                        }
                        gen.Emit(OpCodes.Stfld, fieldInfo);
                    }
                    else
                    {
                        PropertyInfo propertyInfo = (PropertyInfo)member.info;
                        if (member.type.IsValueType)
                        {
                            gen.Emit(OpCodes.Unbox_Any, member.type);
                        }
                        MethodInfo setMethod = propertyInfo.GetSetMethod(true);
                        if (setMethod.IsVirtual)
                        {
                            gen.Emit(OpCodes.Callvirt, setMethod);
                        }
                        else
                        {
                            gen.Emit(OpCodes.Call, setMethod);
                        }
                    }
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.BeginCatchBlock(typeofException);
                    gen.Emit(OpCodes.Stloc_S, e);
                    gen.Emit(OpCodes.Ldstr, "The member value can\'t be unserialized.");
                    gen.Emit(OpCodes.Ldloc_S, e);
                    gen.Emit(OpCodes.Newobj, hproseExceptionCtor);
                    gen.Emit(OpCodes.Throw);
                    gen.Emit(OpCodes.Leave_S, exTryCatch);
                    gen.EndExceptionBlock();
                }
            }
            gen.Emit(OpCodes.Ret);
        }

        private static ObjectUnserializer CreateObjectUnserializer(Type type, string[] names)
        {
            return new ObjectMemberModeUnserializer(type, names);
        }
        public static ObjectUnserializer Get(Type type, string[] names)
        {
            return ObjectUnserializer.Get(HproseMode.MemberMode, type, names, CreateObjectUnserializer);
        }
    }
    internal enum TypeEnum
    {
        Null,
        Object,
        DBNull,
        Boolean,
        Char,
        SByte,
        Byte,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        IntPtr,
        Single,
        Double,
        Decimal,
        DateTime,
        String,
        StringBuilder,
        Guid,
        BigInteger,
        TimeSpan,

        MemoryStream,
        Stream,

        ObjectArray,
        BooleanArray,
        CharArray,
        SByteArray,
        ByteArray,
        Int16Array,
        UInt16Array,
        Int32Array,
        UInt32Array,
        Int64Array,
        UInt64Array,
        IntPtrArray,
        SingleArray,
        DoubleArray,
        DecimalArray,
        DateTimeArray,
        StringArray,
        StringBuilderArray,
        GuidArray,
        BigIntegerArray,
        TimeSpanArray,
        CharsArray,
        BytesArray,

        ICollection,
        IDictionary,
        IList,
        Dictionary,
        List,
        BitArray,
        OtherType,
        OtherTypeArray,
        ArrayList,
        HashMap,
        Hashtable,
        Queue,
        Stack,

        NullableBoolean,
        NullableChar,
        NullableSByte,
        NullableByte,
        NullableInt16,
        NullableUInt16,
        NullableInt32,
        NullableUInt32,
        NullableInt64,
        NullableUInt64,
        NullableIntPtr,
        NullableSingle,
        NullableDouble,
        NullableDecimal,
        NullableDateTime,
        NullableGuid,
        NullableBigInteger,
        NullableTimeSpan,

        GenericList,
        GenericDictionary,
        GenericQueue,
        GenericStack,
        GenericHashMap,
        GenericICollection,
        GenericIDictionary,
        GenericIList,

        ObjectList,
        BooleanList,
        CharList,
        SByteList,
        ByteList,
        Int16List,
        UInt16List,
        Int32List,
        UInt32List,
        Int64List,
        UInt64List,
        IntPtrList,
        SingleList,
        DoubleList,
        DecimalList,
        DateTimeList,
        StringList,
        StringBuilderList,
        GuidList,
        BigIntegerList,
        TimeSpanList,
        CharsList,
        BytesList,

        ObjectIList,
        BooleanIList,
        CharIList,
        SByteIList,
        ByteIList,
        Int16IList,
        UInt16IList,
        Int32IList,
        UInt32IList,
        Int64IList,
        UInt64IList,
        IntPtrIList,
        SingleIList,
        DoubleIList,
        DecimalIList,
        DateTimeIList,
        StringIList,
        StringBuilderIList,
        GuidIList,
        BigIntegerIList,
        TimeSpanIList,
        CharsIList,
        BytesIList,

        StringObjectHashMap,
        ObjectObjectHashMap,
        IntObjectHashMap,
        StringObjectDictionary,
        ObjectObjectDictionary,
        IntObjectDictionary,
        Enum,
        UnSupportedType,
        IpAddress
    }
    internal sealed class HproseFormatter
    {

        public static MemoryStream Serialize(object obj)
        {
            MemoryStream stream = new MemoryStream();
            HproseWriter writer = new HproseWriter(stream);
            writer.Serialize(obj);
            return stream;
        }

        public static void Serialize(object obj, Stream stream)
        {
            HproseWriter writer = new HproseWriter(stream);
            writer.Serialize(obj);
        }

        public static object Unserialize(byte[] data)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize();
        }

        public static object Unserialize(byte[] data, Type type)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize(type);
        }

        public static object Unserialize(Stream stream)
        {
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize();
        }

        public static object Unserialize(Stream stream, Type type)
        {
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize(type);
        }

        public static MemoryStream Serialize(object obj, HproseMode mode)
        {
            MemoryStream stream = new MemoryStream();
            HproseWriter writer = new HproseWriter(stream, mode);
            writer.Serialize(obj);
            return stream;
        }

        public static void Serialize(object obj, Stream stream, HproseMode mode)
        {
            HproseWriter writer = new HproseWriter(stream, mode);
            writer.Serialize(obj);
        }

        public static object Unserialize(byte[] data, HproseMode mode)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize();
        }

        public static object Unserialize(byte[] data, HproseMode mode, Type type)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize(type);
        }

        public static object Unserialize(Stream stream, HproseMode mode)
        {
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize();
        }

        public static object Unserialize(Stream stream, HproseMode mode, Type type)
        {
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize(type);
        }

        public static T Unserialize<T>(byte[] data)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize<T>();
        }

        public static T Unserialize<T>(byte[] data, HproseMode mode)
        {
            MemoryStream stream = new MemoryStream(data);
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize<T>();
        }

        public static T Unserialize<T>(Stream stream)
        {
            HproseReader reader = new HproseReader(stream);
            return reader.Unserialize<T>();
        }

        public static T Unserialize<T>(Stream stream, HproseMode mode)
        {
            HproseReader reader = new HproseReader(stream, mode);
            return reader.Unserialize<T>();
        }
    }
    #endregion


}
