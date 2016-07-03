using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IDWDBClient
{
    public static class ClientExtensions
    {
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
                boat.AddColumn(iable.Name, iable.GetValue(value));
            }
            return query.InsertOrReplace(boat);

        }
    }
}
