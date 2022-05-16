using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DurableFunctionExample {
    public static class Orchestrator {
        static int count = 0;
        static Random random = new Random();

        [FunctionName("Orchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log) {
            count++;
            var outputs = new List<string>();

            /**
             * FUNCTION CHAINING
             */
            // Replace "hello" with the name of your Durable Activity Function.
            log.LogInformation("Output Step 1...");
            outputs.Add(await context.CallActivityAsync<string>("Orchestrator_Hello", "Tokyo"));
            log.LogInformation("End Step 1...");

            log.LogInformation("Output Step 2...");
            outputs.Add(await context.CallActivityAsync<string>("Orchestrator_Hello", "Seattle"));
            log.LogInformation("End Step 2...");

            log.LogInformation("Output Step 3...");
            outputs.Add(await context.CallActivityAsync<string>("Orchestrator_Hello", "London"));
            log.LogInformation("End Step 3...");

            log.LogInformation($"Execution count: {count}");
            /**
             * FUNCTION CHAINING END
             */

            /*
             * FAN OUT
             */
            List<Task> parallelTask = new List<Task>();

            log.LogInformation("Work Start!");
            int work_items = await context.CallActivityAsync<int>("Orchestrator_Workload", "Work Name");

            for (int i = 0; i < work_items; i++) {
                var work = context.CallActivityAsync<int>("Orchestrator_BruteForce", $"Work Item {i}");
                parallelTask.Add(work);
            }

            await Task.WhenAll(parallelTask);

            // Aggregate all N outputs and send the result to F3.
            int c = parallelTask.Count;

            await context.CallActivityAsync("Orchestrator_WorkCompleted", c);

            return outputs;
        }

        [FunctionName("Orchestrator_Hello")]
        public static string SayHello([ActivityTrigger] string name, ILogger log) {
            int sleepy_time = random.Next(20) * 1000;
            log.LogInformation($"{name}: Sleeping for {sleepy_time}");
            Thread.Sleep(sleepy_time);
            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
        }

        [FunctionName("Orchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log) {
            // Function input comes from the request content.
            log.LogInformation("Entry Point...");

            string instanceId = await starter.StartNewAsync("Orchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        [FunctionName("Orchestrator_Workload")]
        public static int Workload([ActivityTrigger] string name, ILogger log) {
            int work = random.Next(100);
            log.LogInformation($"Adding {work} work item(s).");
            return work;
        }

        [FunctionName("Orchestrator_BruteForce")]
        public static void BruteForce([ActivityTrigger] string name, ILogger log) {
            int sleepy_time = random.Next(20) * 1000;
            log.LogInformation($"{name}: Sleeping for {sleepy_time}");
            Thread.Sleep(sleepy_time);
            log.LogInformation($"{name}: Sleeping Complete!");
        }

        [FunctionName("Orchestrator_WorkCompleted")]
        public static void WorkCompleted([ActivityTrigger] int count, ILogger log) {
            log.LogInformation($"Completed {count} work item(s).");
        }
    }
}