using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaListener
{
    class Program
    {
        static void Main(string[] args)
        {
            IEnumerable<KeyValuePair<string, string>> config = BuildConsumerConfig();
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("logistics.instruction.job-state-change-dev");

                while (true)
                {
                    var consumeResult = consumer.Consume(5000);

                    if (consumeResult != null)
                    {
                        consumer.Commit(consumeResult);
                        Console.WriteLine($"message consumed: \n {consumeResult.Message.Value}");
                    }
                    else
                    {
                        Console.WriteLine("No messages. Trying again");
                    }
                }
            }
        }

        private static IEnumerable<KeyValuePair<string, string>> BuildConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = "test",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "test",
                SaslPassword = "test",
                GroupId = "ALESCTESTCONSUMER",
                EnableAutoCommit = false
            };
        }
    }
}
