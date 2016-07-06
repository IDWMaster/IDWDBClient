using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IDWDBClient
{
    public static class ClientExtensions
    {
        /// <summary>
        /// Deserializes this row to the specified type
        /// </summary>
        /// <typeparam name="T">The type to deserialize to</typeparam>
        /// <param name="row">The row</param>
        /// <returns>A new T</returns>
        public static T As<T>(this DataRow row) where T:new()
        {
            T mobile = new T();
            foreach(var column in row.Columns)
            {
                if(mobile.GetType().GetProperty(column.Key) != null)
                {
                    mobile.GetType().GetProperty(column.Key).SetValue(mobile, column.Value);
                }
            }
            var pk = mobile.GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(System.ComponentModel.DataAnnotations.KeyAttribute)).Any());
            if(pk.Any())
            {
                pk.First().SetValue(pk, row.PK);
            }
            return mobile;
        }
        public static TableQuery InsertOrReplace(this TableQuery query, object value)
        {
            var pk = value.GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(System.ComponentModel.DataAnnotations.KeyAttribute)).Any());
            if (!pk.Any())
            {
                throw new Exception("No primary key specified.");
            }
            DataRow boat = new DataRow(pk.First().GetValue(value));

            foreach (var iable in value.GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public))
            {
                if (iable.GetValue(value) != null)
                {
                    boat.AddColumn(iable.Name, iable.GetValue(value));
                }
            }
            return query.InsertOrReplace(boat);

        }
    }
}
