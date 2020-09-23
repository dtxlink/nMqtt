using MqttFx;
using MqttFx.Client;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class MqttFxClientServiceCollectionExtensions
    {
        public static IServiceCollection AddMqttFxClient(this IServiceCollection services, Action<MqttClientOptions> optionsAction)
        {
            if (optionsAction == null)
                throw new ArgumentNullException(nameof(optionsAction));

            services.AddLogging();
            services.AddOptions();
            services.Configure(optionsAction);
            services.AddSingleton<MqttClient>();
            return services;
        }
    }
}
