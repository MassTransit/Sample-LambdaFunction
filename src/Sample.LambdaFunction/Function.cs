using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using MassTransit;
using MassTransit.Transports;
using Microsoft.Extensions.DependencyInjection;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Sample.LambdaFunction
{
    public class Function
    {
        private ServiceProvider _provider;

        // Build up the Host once
        public Function()
        {
            var services = new ServiceCollection();
            services.AddMassTransit(x =>
            {
                x.UsingInMemory();

                x.AddConsumer<SubmitOrderConsumer>();

                x.ConfigureReceiveEndpoint((_, cfg) =>
                {
                    cfg.ClearMessageDeserializers();
                    cfg.UseRawJsonSerializer();
                });
            });
            _provider = services.BuildServiceProvider(true);

        }

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent input, ILambdaContext context)
        {
            using var cts = new CancellationTokenSource(context.RemainingTime);
            using var scope = _provider.CreateScope();

            var ep = scope.ServiceProvider.GetRequiredService<IReceiveEndpointDispatcher<SubmitOrderConsumer>>();

            var headers = new Dictionary<string, object>();

            // foreach (var key in context!.ClientContext!.Environment!.Keys)
            //     headers[key] = context.ClientContext.Environment[key];
            //
            // foreach(var key in context!.ClientContext!.Custom!.Keys)
            //     headers[key] = context.ClientContext.Custom[key];

            foreach (var record in input.Records)
            {
                foreach (var key in record!.Attributes!.Keys)
                    headers[key] = record.Attributes[key];

                foreach (var key in record!.MessageAttributes!.Keys)
                    headers[key] = record.MessageAttributes[key];

                var body = Encoding.UTF8.GetBytes(record.Body);

                await ep.Dispatch(body, headers, cts.Token);
            }
        }
    }

    public class SubmitOrderConsumer : IConsumer<SubmitOrder>
    {
        public Task Consume(ConsumeContext<SubmitOrder> context)
        {
            Console.WriteLine("YUS");
            // TODO: write something to S3
            return Task.CompletedTask;
        }
    }

    public class SubmitOrder
    {

    }
}
