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
    public class Range
    {
        internal object Start;
        internal object End;
        public Range(object start, object end)
        {
            Start = start;
            End = end;
        }
    }
    /// <summary>
    /// Represents a query accessing one or more tables.
    /// </summary>
    public class TableQuery
    {
        internal List<object> fetch_keys = new List<object>();
        internal List<DataRow> insert_rows = new List<DataRow>();
        internal List<Range> fetch_ranges = new List<Range>();
        internal string Name { get; set; }
        /// <summary>
        /// Constructs a table query given a specified table name
        /// </summary>
        /// <param name="name">The name of the table to query</param>
        public TableQuery(string name)
        {
            Name = name;
        }
        public TableQuery Retrieve(Range range)
        {
            fetch_ranges.Add(range);
            return this;
        }
        /// <summary>
        /// Retrieves a list of rows matching the specified keys
        /// </summary>
        /// <param name="keys">The keys to retrieve</param>
        /// <returns></returns>
        public TableQuery Retrieve(IEnumerable<object> keys)
        {
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
            insert_rows.AddRange(rows.Select(m=> { m.TableName = Name;return m; }));
            return this;
        }
        internal byte[] Serialize()
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            mwriter.Write((byte)2);
            if (fetch_keys.Count > 0)
            {
                MemoryStream dstream = new MemoryStream();
                BinaryWriter dwriter = new BinaryWriter(dstream);
                dwriter.Write((byte)0);
                foreach (var iable in fetch_keys)
                {
                    byte[] serialized = DataRow.SerializePK(iable,Name);
                    dwriter.Write((short)serialized.Length);
                    dwriter.Write(serialized);
                }
                byte[] me = dstream.ToArray();
                mwriter.Write(me.Length);
                mwriter.Write(me);
            }
            foreach (var iable in fetch_ranges)
            {
                MemoryStream dstream = new MemoryStream();
                BinaryWriter dwriter = new BinaryWriter(dstream);
                dwriter.Write((byte)3);
                byte[] start = DataRow.SerializePK(iable.Start,Name);
                byte[] end = DataRow.SerializePK(iable.End,Name);
                dwriter.Write((short)start.Length);
                dwriter.Write(start);
                dwriter.Write((short)end.Length);
                dwriter.Write(end);
                byte[] me = dstream.ToArray();
                mwriter.Write(me.Length);
                mwriter.Write(me);
            }
            
            if (insert_rows.Count > 0)
            {
                MemoryStream dstream = new MemoryStream();
                BinaryWriter dwriter = new BinaryWriter(dstream);
                dwriter.Write((byte)1);
                dwriter.Write(insert_rows.Count);
                foreach (var iable in insert_rows)
                {
                    iable.SerializeRow(dwriter);
                }
                byte[] me = dstream.ToArray();
                mwriter.Write(me.Length);
                mwriter.Write(me);
            }



            return mstream.ToArray();
        }

    }
}
