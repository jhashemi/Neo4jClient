using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Neo4j.Driver.V1;
using Neo4jClient.Cypher;
using Neo4jClient.Serialization;
using Newtonsoft.Json;

namespace Neo4jClient
{
    internal static class StatementResultHelper
    {
        internal static string ToJsonString(this INode node, bool inSet = false, bool isNested = false, bool isNestedInList = false)
        {
            var props = node
                .Properties
                .Select(p => $"\"{p.Key}\":{JsonConvert.SerializeObject(p.Value)}");

            if (isNestedInList)
            {
                inSet = true;
                isNested = false;
            }

            if (isNested)
                return $"{{\"data\":{{ {string.Join(",", props)} }}}}";

            if (inSet)
                return $"{string.Join(",", props)}";
            
            return $"{{ {string.Join(",", props)} }}";
        }

        internal static string ToJsonString(this IRelationship node, bool inSet = false, bool isNested = false, bool isNestedInList = false)
        {
            var props = node
                .Properties
                .Select(p => $"\"{p.Key}\":{JsonConvert.SerializeObject(p.Value)}");

            if (isNestedInList)
            {
                inSet = true;
                isNested = false;
            }

            if (isNested)
                return $"{{\"data\":{{ {string.Join(",", props)} }}}}";

            if (inSet)
                return $"{{\"data\": {string.Join(",", props)} }}";

            return $"{{\"data\":{{ {string.Join(",", props)} }}}}";
        }

        internal static string ToJsonString(this object o, bool inSet, bool isNested, bool isNestedInList)
        {
            if (o == null)
                return null;

            if (o is INode)
                return ((INode) o).ToJsonString(inSet, true, isNestedInList);

            if (o is IRelationship)
                return ((IRelationship) o).ToJsonString(inSet, true, isNestedInList);

            if (isNested)
            {
                if (o is IDictionary)
                {
                    var dict = (IDictionary<string, object>) o;
                    var output = new List<string>();
                    foreach (var keyValuePair in dict)
                    {
                        //TODO: Maybe true
                        var s = $"\"{keyValuePair.Key}\":{(keyValuePair.ToJsonString(inSet, true, false) ?? "null")}";
                        output.Add(s);
                    }

                    return string.Join(",", output);
                }

                if (o is KeyValuePair<string, object>)
                {
                    var kvp = (KeyValuePair<string, object>) o;
                    return $"{(kvp.Value.ToJsonString(inSet, true, false)?? "null")}";
//                    return $"\"{kvp.Key}\":{kvp.Value.ToJsonString(inSet, isNested)}";
                }


                if (o.GetType().IsList())
                {
                    var output = new List<string>();
                    foreach (var e in (IEnumerable) o)
                    {
                        if (IsPrimitive(e.GetType()))
                            output.Add($"{e.ToJsonString(true, true, true) ?? "null"}");
                        else
                            output.Add($"{{{e.ToJsonString(true, true, true) ?? "null"}}}");
                    }
                    return $"[{string.Join(",", output)}]";
                }

                return JsonConvert.SerializeObject(o);
            }

            var type = o.GetType();
            if (type.IsList())
            {
                var output = new List<string>();
                foreach (var e in (IEnumerable) o)
                {
                    if (IsPrimitive(e.GetType()))
                        output.Add($"{e.ToJsonString(inSet, false, true) ?? "null"}");
                    else
                        output.Add($"{{\"data\":{e.ToJsonString(inSet, false, true) ?? "null"}}}");
                }
                return $"[{string.Join(",", output)}]";
            }
            return JsonConvert.SerializeObject(o);
        }

        private static string GetColumns(IEnumerable<string> keys)
        {
            return $"\"columns\":[{string.Join(",", keys.Select(k => $"\"{k}\""))}]";
        }

        public static IEnumerable<T> Deserialize<T>(this IRecord record, ICypherJsonDeserializer<T> deserializer, CypherResultMode mode)
        {
            var convertMode = mode;
            var typeT = typeof(T);
            if (!typeT.IsPrimitive() && typeT.GetInterfaces().Contains(typeof(IEnumerable)))
                convertMode = CypherResultMode.Projection;


            var columns = GetColumns(record.Keys);
            //Columns //Data
            var data = new List<string>();
            foreach (var key in record.Keys)
            {
                var o = record[key];
                if (o == null)
                {
                    data.Add(null);
                    continue;
                }

                data.Add(o.ToJsonString(convertMode == CypherResultMode.Set, record.Keys.Count > 1, false));
            }

            var format = "{{ {0}, \"data\":[[ {1} ]] }}";
            var dataJoined = string.Join(",", data.Select(d => d ?? "null"));

            string json;

            switch (mode)
            {
                case CypherResultMode.Set:
                    if(typeT.IsPrimitive())
                        json = string.Format(format, columns, $"{dataJoined}");
                    else
                        json = string.Format(format, columns, $"{{\"data\":{{ {dataJoined} }} }}");
                    break;
                case CypherResultMode.Projection:
                    json = string.Format(format, columns, $"{dataJoined}");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            return deserializer.Deserialize(json);
        }

        public static T Parse<T>(this IRecord record, IGraphClient graphClient)
        {
            if (record.Keys.Count != 1)
                return ParseMutlipleKeys<T>(record, graphClient);

            var identifier = record.Keys.Single();
            return record.Parse<T>(identifier, graphClient);
        }

        private static T ParseMutlipleKeys<T>(IRecord record, IGraphClient graphClient)
        {
            var t = ConstructNew<T>();
            //Collection or Node --- anything else???
            foreach (var property in typeof(T).GetTypeInfo().DeclaredProperties)
            {
                if (!record.Keys.Contains(property.Name))
                    break;

                var method = GetParsed(property.PropertyType);
                var response = method.Invoke(null, new object[] {new Neo4jClientRecord(record, property.Name), graphClient});
                property.SetValue(t, response);
            }
            return t;
        }

        private static T Parse<T>(this IRecord record, string identifier, IGraphClient graphClient)
        {
            var typeT = typeof(T);
            if (typeT.IsPrimitive())
                return record.ParsePrimitive<T>(identifier);

            if (typeT.GetTypeInfo().ImplementedInterfaces.Any(x => x.Name == nameof(IEnumerable)) && typeT.Name != nameof(ExpandoObject))
                return record.ParseCollection<T>(identifier, graphClient);

            var converters = graphClient.JsonConverters;
            converters.Reverse();
            var serializerSettings = new JsonSerializerSettings
            {
                Converters = converters,
                ContractResolver = graphClient.JsonContractResolver
            };

            foreach (var jsonConverter in converters)
                if (jsonConverter.CanConvert(typeof(T)))
                    return JsonConvert.DeserializeObject<T>(record[identifier].As<INode>().ToJsonString(), serializerSettings);


            var t = ConstructNew<T>();
            var obj = record[identifier];
            var node = obj as INode;
            if (node != null)
                foreach (var property in t.GetType().GetProperties())
                    if (node.Properties.ContainsKey(property.Name))
                        if (property.PropertyType.IsPrimitive())
                        {
                            property.SetValue(t, Convert.ChangeType(node.Properties[property.Name], property.PropertyType));
                        }
                        else if (property.PropertyType.GetTypeInfo().ImplementedInterfaces.Any(i => i == typeof(IEnumerable)))
                        {
                            var parsed = GetParsed(property.PropertyType);
                            var enumRecord = new Neo4jClientRecord(node.Properties[property.Name], "Enumerable");
                            var list = parsed.Invoke(null, enumRecord.AsParameters(graphClient));

                            property.SetValue(t, list);
                        }
                        else
                        {
                            var res = JsonConvert.DeserializeObject(
                                $"\"{node.Properties[property.Name].As<string>()}\"",
                                property.PropertyType,
                                serializerSettings);
                            property.SetValue(t, res);
                        }
            return t;
        }

        private static T ParseCollection<T>(this IRecord record, string identifier, IGraphClient graphClient)
        {
            var typeT = typeof(T).GetTypeInfo();
            if (!typeT.IsGenericType && !typeT.IsArray)
                throw new InvalidOperationException($"Don't know how to handle {typeof(T).FullName}");

            if (typeT.IsArray)
                return record.ParseArray<T>(identifier, graphClient);

            var genericArgs = typeT.GenericTypeArguments;
            if (genericArgs.Length > 1)
                throw new InvalidOperationException($"Don't know how to handle {typeof(T).FullName}");

            var listType = typeof(List<>).MakeGenericType(genericArgs.Single());
            var list = Activator.CreateInstance(listType);

            foreach (var item in (IEnumerable) record[identifier])
            {
                var internalRecord = new Neo4jClientRecord(item, identifier);
                var method = GetParsed(genericArgs.Single());
                var parsed = method.Invoke(null, internalRecord.AsParameters(graphClient));
                listType.GetMethod("Add").Invoke(list, new[] {parsed});
            }

            return (T) list;
        }

        private static T ParseArray<T>(this IRecord record, string identifier, IGraphClient graphClient)
        {
            var typeT = typeof(T).GetTypeInfo();
            if (!typeT.IsArray)
                throw new InvalidOperationException($"Don't know how to handle {typeof(T).FullName}");

            var arrayElementType = typeT.GetElementType();
            var listType = typeof(List<>).MakeGenericType(arrayElementType);

            var method = typeof(StatementResultHelper).GetMethod(nameof(ParseCollection), BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.FlattenHierarchy)
                .MakeGenericMethod(listType);
            dynamic listVersion = method.Invoke(null, new object[] {record, identifier, graphClient});

            return listVersion.ToArray();
        }

        private static MethodInfo GetParsed(Type genericParameter)
        {
            return typeof(StatementResultHelper).GetMethod("Parse").MakeGenericMethod(genericParameter);
        }

        public static string ParseAnonymous(this IRecord record, IGraphClient graphClient, bool onlyReturnData = false)
        {
            return JsonConvert.SerializeObject(ParseAnonymousAsDynamic(record, graphClient, onlyReturnData));
        }

        private static dynamic ParseAnonymousAsDynamic(this IRecord record, IGraphClient graphClient, bool onlyReturnData)
        {
            var data = new List<dynamic>();

            var inner = new List<dynamic>();
            foreach (var identifier in record.Keys)
            {
                dynamic expando = new ExpandoObject();
                var t = (IDictionary<string, object>) expando;
                var obj = record[identifier];
                var node = obj as INode;
                if (node != null)
                {
                    foreach (var property in node.Properties)
                        t[property.Key] = property.Value;
                    inner.Add(new Dictionary<string, dynamic> {{"data", expando}});
                }
                else if (obj is IEnumerable && !(obj is string))
                {
                    var count = 0;

                    foreach (var o in (IEnumerable) obj)
                    {
                        var newRecord = new Neo4jClientRecord(o, identifier);
                        var p2 = ParseAnonymousAsDynamic(newRecord, graphClient, true);
                        inner.Add(p2);
                        count++;
                    }
                    if (count == 0)
                        inner.Add(new object[0]);
                }
                else
                {
                    inner.Add(record.Parse<string>(identifier, graphClient));
                }
            }

            data.Add(inner);

            if (onlyReturnData)
                return inner;

            //TODO: Ugh! this is about as hacky as it can get
            dynamic output = new
            {
                columns = new List<string>(record.Keys),
                data
            };

            return output;
        }

        // private static IList<dynamic> ParseAnonymousCollection()

        public static bool IsAnonymous(this Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var hasCompilerGenerated = type.GetTypeInfo().GetCustomAttribute<CompilerGeneratedAttribute>() != null;

            // HACK: The only way to detect anonymous types right now.
            return hasCompilerGenerated
                   && type.GetTypeInfo().IsGenericType && type.Name.Contains("AnonymousType")
                   && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                   && (type.GetTypeInfo().Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        private static T ConstructNew<T>()
        {
            try
            {
                return (T) typeof(T).GetTypeInfo().DeclaredConstructors.First(c => c.GetParameters().Length == 0).Invoke(new object[] { });
            }
            catch (NullReferenceException e)
            {
                throw new InvalidCastException("Unable to create an instance of " + typeof(T).Name + " without a parameterless constructor.", e);
            }
        }

        private static bool IsList(this Type type)
        {
            return !type.IsPrimitive() && type.GetInterfaces().Contains(typeof(IEnumerable));
        }

        private static bool IsPrimitive(this Type type)
        {
            return type.GetTypeInfo().IsPrimitive || type == typeof(string) || type == typeof(decimal);
        }

        private static T ParsePrimitive<T>(this IRecord record, string identifier)
        {
            return (T) record[identifier];
        }

        private class Neo4jClientRecord : IRecord
        {
            public Neo4jClientRecord(object obj, string identifier)
            {
                Values = new Dictionary<string, object> {{identifier, obj}};
            }

            public Neo4jClientRecord(IRecord record, string identifier)
            {
                Values = new Dictionary<string, object> {{identifier, record[identifier]}};
            }

            object IRecord.this[int index]
            {
                get { throw new NotImplementedException("This should not be called."); }
            }

            object IRecord.this[string key] => Values[key];

            public IReadOnlyDictionary<string, object> Values { get; }
            public IReadOnlyList<string> Keys => Values.Keys.ToList();

            public object[] AsParameters(IGraphClient graphClient)
            {
                return new object[] {this, graphClient};
            }
        }
    }
}