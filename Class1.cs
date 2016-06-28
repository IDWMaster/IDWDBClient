/*
 This file is part of IDWDB Client Driver.
    IDWDB Client Driver is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    IDWDB Client Driver is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with IDWDB Client Driver.  If not, see <http://www.gnu.org/licenses/>.
 * */


/*!
*    \mainpage IDWDB Documentation
*    \section intro_sec Introduction
*    IDWDB is a fully-managed database PaaS (Platform-as-a-Service) offering. IDWDB is a NoSQL, highly scalable database. With IDWDB, you simply select a usage tier, based on expected consumer demand, and we handle the rest! IDWDB can dynamically scale based on customer demand, and clusters are self-healing, to prevent any downtime or loss of data in the event of a single-server failure.
*    <h3>Queries</h3>
*    IDWDB supports the following basic query operations: 
*        - Retrieve records based on list of primary keys (highly efficient; will only query servers in the cluster which hold effected records)
*        - Retrieve records (less efficient; same query will be dispatched to multiple servers in a cluster)
*        - Insert list of records
*        - Delete list of records based on primary keys (highly efficient; will only query servers in the cluster which hold effected records)
*        - Delete range of records (less efficient in transaction queries)
*        - Batch query (some combination of the above operations; typically more efficient than sending out multiple queries)
*    <h3>Transactions</h3>
*    By default; each write to a row in the database is an atomic operation. If multiple write operations are batched in a single query; some operations may succeed, and others may fail; potentially leaving the database in an inconsistent state. To avoid this problem; it is possible to enable support for transactions in the client library. Transactions may be enabled by setting IDWDBClient.DatabaseClient.TransactionsEnabled to true. To begin a new transaction; you may call IDWDBClient.DatabaseClient.BeginTransaction to start a new transaction, and IDWDBClient.DatabaseClient.EndTransaction to commit (or rollback) a transaction in the database. Using transactions is only advised when absolutely necessary; as transactions can significantly decrease the amount of parallelism in the database. If you don't need transactional consistency, it is recommended to leave them disabled. Transactions can be enabled/disabled quickly on a per-connection level.
*    <h3>Using IDWDB as a relational/table-driven database</h3>
*    IDWDB is exposed as a table-driven database to the client, via the IDWDBClient.TableQuery class. Table queries allow you to query the key-value store database as if it is a collection of tables, simplifying many common database programming tasks.
* 
*/



using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Security;
using System.Security.Cryptography;
using System.IO;
namespace IDWDBClient
{
    namespace Internals
    {
        internal class AsyncTCPSocket : IDisposable
        {
            bool opInProgress = false;
            TcpClient s;
            Stream str;
            IDWNatives.FastNetworkStream fs;
            public AsyncTCPSocket(TcpClient s)
            {
                //str = s.GetStream();
                this.s = s;
                fs = new IDWNatives.FastNetworkStream((ulong)s.Client.Handle.ToInt64());
            }
            public Task SendAsync(byte[] buffer, int offset, int count)
            {
                // return str.WriteAsync(buffer, offset, count);

                if (offset < 0)
                {
                    throw new IndexOutOfRangeException("Buffer underflow? Anyone?");
                }
                if (offset + count > buffer.Length)
                {
                    throw new IndexOutOfRangeException("offset+count must be less than length.");
                }
                if (opInProgress)
                {

                    throw new IOException("I/O operation already pending");
                }
                TaskCompletionSource<bool> tm = new TaskCompletionSource<bool>();
                opInProgress = true;

                fs.SendAsync(buffer, offset, count, m => {

                    opInProgress = false;
                    if (m != count)
                    {
                        tm.SetException(new IOException("Transmit error"));
                    }
                    else
                    {
                        tm.SetResult(true);
                    }
                });
                return tm.Task;
            }
            public Task<int> ReceiveAsync(byte[] buffer, int offset, int count)
            {
                // return str.ReadAsync(buffer, offset, count);

                if (offset < 0)
                {
                    throw new IndexOutOfRangeException("Buffer underflow? Anyone?");
                }
                if (offset + count > buffer.Length)
                {
                    throw new IndexOutOfRangeException("offset+count must be less than length.");
                }
                if (opInProgress)
                {
                    throw new IOException("I/O operation already pending");
                }
                TaskCompletionSource<int> tm = new TaskCompletionSource<int>();
                opInProgress = true;
                fs.ReceiveAsync(buffer, offset, count, m => {
                    opInProgress = false;
                    tm.SetResult(m);

                });
                return tm.Task;
            }

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        // TODO: dispose managed state (managed objects).
                    }

                    s.Close();
                    disposedValue = true;
                }
            }

            // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
            // ~AsyncTCPSocket() {
            //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            //   Dispose(false);
            // }

            // This code added to correctly implement the disposable pattern.
            public void Dispose()
            {
                // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
                Dispose(true);
                // TODO: uncomment the following line if the finalizer is overridden above.
                // GC.SuppressFinalize(this);
            }
            #endregion

        }

    }
  
    internal class AsyncMessageStream : IDisposable
    {
        Internals.AsyncTCPSocket str;
        public AsyncMessageStream(Internals.AsyncTCPSocket str)
        {
            this.str = str;
        }

        public void Dispose()
        {
            str.Dispose();
        }

        public async Task<byte[]> ReadMessage()
        {
            byte[] buffy = new byte[4];
            int mlen = await str.ReceiveAsync(buffy, 0, 4);
            if (mlen == 0)
            {
                return null;
            }
            int len = new BinaryReader(new MemoryStream(buffy)).ReadInt32();
            buffy = new byte[len];
            int received = 0;
            while (received < len)
            {
                received += await str.ReceiveAsync(buffy, received, len - received);
            }
            return buffy;
        }
        public async Task SendMessage(byte[] msg)
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            mwriter.Write(msg.Length);
            mwriter.Write(msg);
            msg = mstream.ToArray();
            await str.SendAsync(msg, 0, msg.Length);
        }
    }
    
    /// <summary>
    /// A row of data
    /// </summary>
    public class DataRow
    {
        internal static byte[] SerializePK(object pk, string tableName)
        {

            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            mwriter.Write(tableName);
            mwriter.Write(pk.Serialize());
            return mstream.ToArray();
        }
        byte[] SerializePK()
        {
            return SerializePK(PK, TableName);
        }
        public DataRow AddColumn(string name, object value)
        {
            this[name] = value;
            return this;
        }
        internal void SerializeRow(BinaryWriter mwriter)
        {
            byte[] pk_serialized = SerializePK();
            if(pk_serialized.Length>=1000)
            {
                throw new InvalidDataException("PK field must be <=1000 bytes");
            }
            mwriter.Write((short)pk_serialized.Length);
            mwriter.Write(pk_serialized);
            MemoryStream dstream = new MemoryStream();
            BinaryWriter dwriter = new BinaryWriter(dstream);
            foreach(var iable in columns)
            {
                dwriter.Write(iable.Key);
                iable.Value.Serialize(dwriter);
            }
            mwriter.Write((int)dstream.Length);
            mwriter.Write(dstream.ToArray());
        }
        internal byte[] SerializeRow()
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            SerializeRow(mwriter);
            return mstream.ToArray();
        }

        internal static DataRow DeserializeRow(BinaryReader mreader)
        {
            byte[] pk = mreader.ReadBytes(mreader.ReadInt16());
            BinaryReader dreader = new BinaryReader(new MemoryStream(pk));
            string tableName = dreader.ReadString();
            DataRow boat = new DataRow(DataFormats.Deserialize(dreader));
            boat.TableName = tableName;

            mreader = new BinaryReader(new MemoryStream(mreader.ReadBytes(mreader.ReadInt32())));

            while (mreader.BaseStream.Length != mreader.BaseStream.Position)
            {
                boat[mreader.ReadString()] = DataFormats.Deserialize(mreader);
            }
            return boat;
        }
        /// <summary>
        /// Constructs a new DataRow
        /// </summary>
        /// <param name="pk">The primary key for this row</param>
        public DataRow(object pk)
        {
            this.PK = pk;
        }

        Dictionary<string, object> columns = new Dictionary<string, object>();
        /// <summary>
        /// The primary key associated with this row. Primary keys MUST be less than 1000 bytes long!
        /// </summary>
        public object PK { get; set; }
        /// <summary>
        /// The name of the table from which this row of data originated.
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// Retrieves a column given by a specified name
        /// </summary>
        /// <param name="idx">The name of the column to retrieve</param>
        /// <returns></returns>
        public object this[string idx]
        {
            get
            {
                return columns.ContainsKey(idx) ? columns[idx] : null;
            }set
            {
                columns[idx] = value;
            }
        }

        /// <summary>
        /// Retrieves a list of columns as key-value pairs
        /// </summary>
        public IEnumerable<KeyValuePair<string,object>> Columns
        {
            get
            {
                return columns;
            }
        }
    }
    /// <summary>
    /// A database client; representing a connection to a IDWDB database instance.
    /// </summary>
    public class DatabaseClient : IDisposable
    {

        
        public void Dispose()
        {

        }
        ICryptoTransform encryptor;
        ICryptoTransform decryptor;
        async Task<byte[]> ReceiveEncryptedMessage()
        {
            byte[] encrypted = await clearStream.ReadMessage();
            if (localSecret != null)
            {
                return encrypted;
            }
            decryptor.TransformBlock(encrypted, 0, encrypted.Length, encrypted, 0);
            MemoryStream mstream = new MemoryStream(encrypted);
            BinaryReader mreader = new BinaryReader(mstream);
            return mreader.ReadBytes(mreader.ReadInt32());
        }
        async Task SendEncryptedMessage(byte[] msg)
        {

            if (localSecret == null)
            {
                MemoryStream mstream = new MemoryStream();
                BinaryWriter mwriter = new BinaryWriter(mstream);
                mwriter.Write(msg.Length);
                mwriter.Write(msg);
                int alignedSize = (int)mstream.Length;
                alignedSize += 16 - (alignedSize % 16); //Align to 16-byte boundary
                mstream.Position = 0;
                msg = new byte[alignedSize];
                mstream.Read(msg, 0, msg.Length);
                encryptor.TransformBlock(msg, 0, msg.Length, msg, 0);
            }
            await clearStream.SendMessage(msg);

        }
        public  Task RunQuery(TableQuery query, Query_OnResultsSync callback)
        {
            return RunQuery(query, async m => {
                return callback(m);
            });
        }

        public delegate bool Query_OnResultsSync(IEnumerable<DataRow> rows);

        public delegate Task<bool> Query_OnResults(IEnumerable<DataRow> rows);
        public async Task RunQuery(TableQuery query,Query_OnResults callback)
        {
            byte[] request = query.Serialize();
            await SendEncryptedMessage(request);
            request = null;
            while (true)
            {
                byte[] msg = await ReceiveEncryptedMessage();
                if(msg == null)
                {
                    throw new IOException("The remote host has closed the connection.");
                }
                if(msg.Length == 0)
                {
                    return;
                }
                MemoryStream mstream = new MemoryStream(msg);
                BinaryReader mreader = new BinaryReader(mstream);
                List<DataRow> rows = new List<DataRow>();
                while (mstream.Position != mstream.Length)
                {
                    DataRow boat = DataRow.DeserializeRow(mreader);
                    rows.Add(boat);
                }
                await SendEncryptedMessage(new byte[] { await callback(rows) ? (byte)1 : (byte)0 });
                
            }
        }
        string hostName;
        int portno;
        byte[] clientKey;
        byte[] serverKey;

        byte[] localSecret;
        /// <summary>
        /// DON'T USE THIS! This constructor is for internal testing purposes only and will not work on your local machine!
        /// </summary>
        /// <param name="localSecret">Internal testing purposes only</param>
        public DatabaseClient(byte[] localSecret)
        {
            this.localSecret = localSecret;
            hostName = "127.0.0.1";
            portno = 3883;
        }
        internal DatabaseClient(DatabaseClient other)
        {
            localSecret = other.localSecret;
            hostName = other.hostName;
            portno = other.portno;
            serverKey = other.serverKey;
            clientKey = other.clientKey;
        }
        /// <summary>
        /// Creates a new connection to a database
        /// </summary>
        /// <param name="endpoint">The endpoint to connect to</param>
        /// <param name="serverKey">The server's public key</param>
        /// <param name="clientKey">The private key to use for authentication</param>
        public DatabaseClient(string endpoint, byte[] serverKey, byte[] clientKey)
        {
            this.serverKey = serverKey;
            this.clientKey = clientKey;
            try
            {
                if (endpoint.Contains(":"))
                {
                    string[] er = endpoint.Split(':');
                    hostName = er[0];
                    portno = int.Parse(er[1]);
                }
                else
                {
                    hostName = endpoint;
                    portno = 3883;
                }
            }
            catch (Exception er)
            {
                throw new InvalidOperationException("Invalid endpoint address.");
            }
        }



        Aes cryptoProvider;
        AsyncMessageStream clearStream;
        async Task Reconnect()
        {
            try
            {
                clearStream.Dispose();
            }
            catch (Exception er)
            {

            }
            try
            {
                if (cryptoProvider != null)
                {
                    cryptoProvider.Dispose();
                }
            }
            catch (Exception er)
            {
            }
            await ConnectAsync();
        }

        /// <summary>
        /// Establishes a connection to the database server
        /// </summary>
        public async Task ConnectAsync()
        {
            if (localSecret != null)
            {
                TcpClient mclient = new TcpClient();
                await mclient.ConnectAsync(hostName, portno);
                clearStream = new AsyncMessageStream(new Internals.AsyncTCPSocket(mclient));
                
                await clearStream.SendMessage(localSecret);
                return;
            }

            using (RSACryptoServiceProvider serverRsa = new RSACryptoServiceProvider())
            {
                serverRsa.ImportCspBlob(serverKey);
                TcpClient mclient = new TcpClient();
                await mclient.ConnectAsync(hostName, portno);
                
                clearStream = new AsyncMessageStream(new Internals.AsyncTCPSocket(mclient));
                MemoryStream mstream = new MemoryStream();
                BinaryWriter mwriter = new BinaryWriter(mstream);
                using (RandomNumberGenerator mrand = new RNGCryptoServiceProvider())
                {
                    byte[] challenger = new byte[16];
                    mrand.GetBytes(challenger);
                    //Write RSA thumbprint+challenge
                    using (RSACryptoServiceProvider rsa = new RSACryptoServiceProvider())
                    {
                        rsa.ImportCspBlob(clientKey);
                        using (SHA512CryptoServiceProvider sha = new SHA512CryptoServiceProvider())
                        {
                            byte[] keyhash = sha.ComputeHash(rsa.ExportCspBlob(false));
                            mwriter.Write(keyhash, 0, 16);
                            mwriter.Write(challenger);
                            byte[] encrypted = serverRsa.Encrypt(mstream.ToArray(), true);
                            
                            await clearStream.SendMessage(encrypted);
                            //Get reply containing cryptographic key+challenge response
                            
                            mstream = new MemoryStream(await clearStream.ReadMessage());
                           
                            BinaryReader mreader = new BinaryReader(new MemoryStream(rsa.Decrypt(mstream.ToArray(), true)));
                            byte[] sessionKey = mreader.ReadBytes(32);
                            if (new Guid(mreader.ReadBytes(16)) != new Guid(challenger))
                            {
                                throw new SecurityException("It seems that somebody wants to hack you.... Or there's a server problem. Who knows?");
                            }
                            cryptoProvider = new AesCryptoServiceProvider();
                            cryptoProvider.Key = sessionKey;
                            cryptoProvider.IV = new byte[16];
                            cryptoProvider.Padding = PaddingMode.None;
                            encryptor = cryptoProvider.CreateEncryptor();
                            decryptor = cryptoProvider.CreateDecryptor();
                            await SendEncryptedMessage(new byte[0]);
                        }
                    }
                }

            }

        }
    }
}