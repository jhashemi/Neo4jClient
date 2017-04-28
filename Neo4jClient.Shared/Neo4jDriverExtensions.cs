using System.Collections.Generic;
using System.Reflection;
using Neo4j.Driver.V1;
using Neo4jClient.Cypher;
using Newtonsoft.Json;

namespace Neo4jClient
{
    public static class Neo4jDriverExtensions
    {
        public static IStatementResult Run(this ISession session, CypherQuery query, IGraphClient gc)
        {
            return session.Run(query.QueryText, query.ToNeo4jDriverParameters(gc));
        }

        public static IStatementResult Run(this ITransaction session, CypherQuery query, IGraphClient gc)
        {
            return session.Run(query.QueryText, query.ToNeo4jDriverParameters(gc));
        }

        // ReSharper disable once InconsistentNaming
        public static Dictionary<string, object> ToNeo4jDriverParameters(this CypherQuery query, IGraphClient gc)
        {
            var output = new Dictionary<string, object>();

            foreach (var item in query.QueryParameters)
            {
                var type = item.Value.GetType();
                var typeInfo = type.GetTypeInfo();

                if (typeInfo.IsClass && type != typeof(string))
                    output.Add(item.Key, JsonConvert.DeserializeObject<Dictionary<string, object>>(JsonConvert.SerializeObject(item.Value, Formatting.None, gc.JsonConverters.ToArray())));
                else
                    output.Add(item.Key, item.Value);
            }

            return output;
        }
    }
}