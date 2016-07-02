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




using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
namespace IDWDBClient
{
    /// <summary>
    /// Represents a Range; used in a Range query.
    /// </summary>
    public class Range
    {
        internal object Start;
        internal object End;
        /// <summary>
        /// Creates a new Rage object
        /// </summary>
        /// <param name="start">The starting element</param>
        /// <param name="end">The ending element</param>
        public Range(object start, object end)
        {
            Start = start;
            End = end;
        }

        /// <summary>
        /// Creates a Range containing all matching entries starting with the specified string
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Range StartsWith(string value)
        {
            StringBuilder upperBound = new StringBuilder(value);
            upperBound[upperBound.Length - 1]++;
            return new Range(value, upperBound.ToString());
        }
    }
    enum QueryOpType
    {
        FetchKeys, InsertRows, FetchRange, UseTransaction, Delete, RangeDelete
    }
    internal class QueryOperation
    {
        public QueryOpType type;
        public object value;
        public QueryOperation(string tableName)
        {
            Name = tableName;
        }
        //Table name
        public string Name;
        public byte[] Serialize()
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter dwriter = new BinaryWriter(mstream);
            switch(type)
            {
                case QueryOpType.FetchKeys:
                    {
                        List<object> fetch_keys = value as List<object>;
                        dwriter.Write((byte)0);
                        foreach (var iable in fetch_keys)
                        {
                            byte[] serialized = DataRow.SerializePK(iable, Name);
                            dwriter.Write((short)serialized.Length);
                            dwriter.Write(serialized);
                        }
                    }
                    break;
                case QueryOpType.FetchRange:
                    {

                        dwriter.Write((byte)3);
                        Range iable = value as Range;
                        
                            byte[] start = DataRow.SerializePK(iable.Start, Name);
                            byte[] end = DataRow.SerializePK(iable.End, Name);
                            dwriter.Write((short)start.Length);
                            dwriter.Write(start);
                            dwriter.Write((short)end.Length);
                            dwriter.Write(end);
                        

                    }
                    break;
                case QueryOpType.InsertRows:
                    {
                        List<DataRow> insert_rows = value as List<DataRow>;
                        dwriter.Write((byte)1);
                        dwriter.Write(insert_rows.Count);
                        foreach (var iable in insert_rows)
                        {
                            iable.SerializeRow(dwriter);
                        }
                    }
                    break;
                case QueryOpType.UseTransaction:
                    {
                        dwriter.Write((byte)6);
                    }
                    break;
                case QueryOpType.Delete:
                    {
                        List<object> delete_keys = value as List<object>;
                        dwriter.Write((byte)4);
                        foreach(var iable in delete_keys)
                        {
                            byte[] key = DataRow.SerializePK(iable, Name);
                            dwriter.Write((short)key.Length);
                            dwriter.Write(key);
                        }
                    }
                    break;
                case QueryOpType.RangeDelete:
                    {
                        Range er = value as Range;
                        dwriter.Write((byte)5);
                        byte[] start = DataRow.SerializePK(er.Start, Name);
                        byte[] end = DataRow.SerializePK(er.End, Name);
                        dwriter.Write((short)start.Length);
                        dwriter.Write(start);
                        dwriter.Write((short)end.Length);
                        dwriter.Write(end);
                    }
                    break;
            }
            return mstream.ToArray();
        }
    }
    /// <summary>
    /// Represents a query accessing one or more tables.
    /// </summary>
    public class TableQuery
    {

        internal List<QueryOperation> ops = new List<QueryOperation>();
        QueryOperation Last
        {
            get
            {
                if (ops.Count == 0)
                {
                    return null;
                }
                return ops[ops.Count - 1];
            }
        }
        internal string Name { get; set; }
        /// <summary>
        /// Constructs a table query given a specified table name
        /// </summary>
        /// <param name="name">The name of the base table to query</param>
        public TableQuery(string name)
        {
            Name = name;
        }
       
        public TableQuery UseTransaction()
        {
            ops.Add(new QueryOperation(Name) { type = QueryOpType.UseTransaction });
            return this;
        }

        /// <summary>
        /// Retrieves a list of data given by the specified range (exclusive)
        /// </summary>
        /// <param name="range">The range to retrieve</param>
        /// <returns></returns>
        public TableQuery Retrieve(Range range)
        {
            ops.Add(new QueryOperation(Name) { type = QueryOpType.FetchRange, value = range });
            
            return this;
        }
        /// <summary>
        /// Deletes all values in the specified range (exclusive)
        /// </summary>
        /// <param name="range">The range to delete</param>
        /// <returns></returns>
        public TableQuery Delete(Range range)
        {
            ops.Add(new QueryOperation(Name) { type = QueryOpType.RangeDelete, value = range });
            return this;
        }

        /// <summary>
        /// Deletes a list of items, by primary key
        /// </summary>
        /// <param name="keys">The keys to delete</param>
        /// <returns></returns>
        public TableQuery Delete(IEnumerable<object> keys)
        {
            List<object> delete_keys = new List<object>();
            if (Last?.type == QueryOpType.Delete)
            {
                delete_keys = Last.value as List<object>;
            }
            else
            {
                ops.Add(new QueryOperation(Name) { type = QueryOpType.Delete, value = delete_keys });
            }
            delete_keys.AddRange(keys);
            return this;
        }


        /// <summary>
        /// Retrieves a list of rows matching the specified keys
        /// </summary>
        /// <param name="keys">The keys to retrieve</param>
        /// <returns></returns>
        public TableQuery Retrieve(IEnumerable<object> keys)
        {
            List<object> fetch_keys = new List<object>();
            if (Last?.type == QueryOpType.FetchKeys)
            {
                fetch_keys = Last.value as List<object>;
            }
            else
            {
                ops.Add(new QueryOperation(Name) { type = QueryOpType.FetchKeys, value = fetch_keys });
            }
            fetch_keys.AddRange(keys);
            return this;
        }
        /// <summary>
        /// Retrieves a list of rows matching the specified keys
        /// </summary>
        /// <param name="keys">The keys to retrieve</param>
        /// <returns></returns>
        public TableQuery Retrieve(params object[] keys)
        {
            return Retrieve(keys as IEnumerable<object>);
        }
        /// <summary>
        /// Inserts a series of rows; or replace them if they already exist.
        /// </summary>
        /// <param name="rows">The rows to insert</param>
        /// <returns></returns>
        public TableQuery InsertOrReplace(IEnumerable<DataRow> rows)
        {
            List<DataRow> insert_rows = new List<DataRow>();
            if (Last?.type == QueryOpType.InsertRows)
            {
                insert_rows = Last.value as List<DataRow>;
            }else
            {
                ops.Add(new QueryOperation(Name) { type = QueryOpType.InsertRows,value = insert_rows });
            }
            insert_rows.AddRange(rows.Select(m=> { m.TableName = Name;return m; }));
            return this;
        }
        /// <summary>
        /// Inserts a series of rows; or replace them if they already exist.
        /// </summary>
        /// <param name="rows">The rows to insert</param>
        public TableQuery InsertOrReplace(params DataRow[] rows)
        {
            return InsertOrReplace(rows as IEnumerable<DataRow>);
        }
        internal byte[] Serialize()
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            mwriter.Write((byte)2);
            foreach(var iable in ops)
            {
                byte[] serialized = iable.Serialize();
                mwriter.Write(serialized.Length);
                mwriter.Write(serialized);
            }
            



            return mstream.ToArray();
        }

    }
}
