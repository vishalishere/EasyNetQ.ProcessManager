using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.ProcessManager;
using EasyNetQ.ProcessManager.State.SqlServer;
using EasyNetQ.Topology;
using Microsoft.FSharp.Core;

namespace Process3
{
    class Program
    {
        static void Main(string[] args)
        {
            var rabbitConnString = ConfigurationManager.AppSettings["rabbit connection"];
            var sqlConnString = ConfigurationManager.AppSettings["sql connection"];
            var bus = RabbitHutch.CreateBus(rabbitConnString);
            var active = new SqlActiveStore(sqlConnString);
            var store = new SqlStateStore(sqlConnString, new Serializer());
            var pm = new ProcessManager(new EasyNetQPMBus(bus), "Process", active, store);
            Workflow.Configure(pm);

            var d = new Dictionary<string, object> {{"name", "bob"}};
            Enumerable.Range(1, 100).ToList().ForEach(i =>
            {
                Task.Delay(1000).Wait();
                Console.WriteLine("Start email {0}", i);
                pm.Start(state => Workflow.Start(d, "hello {{ name }}", "{{ name }}@example.com"));
            });
            Console.ReadLine();
            bus.Dispose();
        }
    }
}
