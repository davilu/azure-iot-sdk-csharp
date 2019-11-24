using Microsoft.Azure.Devices.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.E2ETests
{
    [TestClass]
    [TestCategory("IoTHub-E2E")]
    [Ignore]
    public class DeviceClientManualE2ETests : IDisposable
    {
        private const int s_repeat = 10;
        private const string s_methodName = "testMethod";

        private static readonly string DevicePrefix = $"E2E_{nameof(DeviceClientManualE2ETests)}_";
        private static readonly TestLogging _log = TestLogging.GetInstance();
        private readonly ConsoleEventListener _listener;

        public DeviceClientManualE2ETests()
        {
            _listener = TestConfig.StartEventListener();
        }

        #region manual test: check connection leaking on open-close
        [TestMethod]
        public async Task OpenClose_AMQP()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Amqp).ConfigureAwait(false);
        }
        [TestMethod]
        public async Task OpenClose_AMQP_TCP()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Amqp_Tcp_Only).ConfigureAwait(false);
        }
        [TestMethod]
        public async Task OpenClose_AMQP_WebSocket()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Amqp_WebSocket_Only).ConfigureAwait(false);
        }
        public async Task OpenClose_Mqtt()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Mqtt).ConfigureAwait(false);
        }
        [TestMethod]
        public async Task OpenClose_Mqtt_TCP()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Mqtt_Tcp_Only).ConfigureAwait(false);
        }
        [TestMethod]
        public async Task OpenClose_Mqtt_WebSocket()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            await OpenCloseAsync(testDevice, Client.TransportType.Mqtt_WebSocket_Only).ConfigureAwait(false);
        }
        #endregion

        #region manual test: check connection recovery on network outage
        [TestMethod]
        public async Task NoNetwork_Recover()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(DevicePrefix, TestDeviceType.Sasl).ConfigureAwait(false);
            DeviceClient deviceClient = testDevice.CreateDeviceClient(Client.TransportType.Amqp);

            using (deviceClient) {
                TimeSpan waitTime = TimeSpan.FromMinutes(8);

                ConnectionStatus connectionStatus;
                ConnectionStatusChangeReason changeReason;
                deviceClient.SetConnectionStatusChangesHandler((status, reason) =>
                {
                    _log.WriteLine($"Device {testDevice.Id} statuse changed to {status}, reason: {reason}.");
                    connectionStatus = status;
                    changeReason = reason;
                });

                await deviceClient.SetMethodHandlerAsync(
                    s_methodName,
                    (methodRequest, context) => {
                        MethodResponse response = new MethodResponse(
                            methodRequest.Data,
                            200
                        );
                        return Task.FromResult(response);
                    },
                    deviceClient
                ).ConfigureAwait(false);

                _log.WriteLine($"Invoking direct method to verify device is online.");
                await InvokeDirectMethodAndVerifyAsync(testDevice.Id).ConfigureAwait(false);

                _log.WriteLine("###################### Turn off network ######################");
                await Task.Delay(waitTime).ConfigureAwait(false);
                _log.WriteLine("###################### Turn network back on ######################");
                await Task.Delay(TimeSpan.FromMinutes(1)).ConfigureAwait(false);
                _log.WriteLine($"Invoking direct method to verify device back online.");
                await InvokeDirectMethodAndVerifyAsync(testDevice.Id).ConfigureAwait(false);
                await deviceClient.CloseAsync().ConfigureAwait(false);
            }
        }
        #endregion

        public static async Task InvokeDirectMethodAndVerifyAsync(string deviceId)
        {
            string payload = $"{{\"key\":\"{Guid.NewGuid()}\"}}";
            using (ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(Configuration.IoTHub.ConnectionString))
            {
                _log.WriteLine($"{nameof(InvokeDirectMethodAndVerifyAsync)}: Invoke method {s_methodName}.");
                CloudToDeviceMethodResult response =
                    await serviceClient.InvokeDeviceMethodAsync(
                        deviceId,
                        new CloudToDeviceMethod(s_methodName, TimeSpan.FromMinutes(5)).SetPayloadJson(payload)).ConfigureAwait(false);

                _log.WriteLine($"{nameof(InvokeDirectMethodAndVerifyAsync)}: Method status: {response.Status}.");
                Assert.AreEqual(200, response.Status, $"The expected respose status should be 200 but was {response.Status}");
                Assert.AreEqual(payload, response.GetPayloadAsJson(), $"The expected respose payload should be {payload} but was {payload}");

                await serviceClient.CloseAsync().ConfigureAwait(false);
            }
        }

        private static async Task OpenCloseAsync(TestDevice testDevice, Client.TransportType transportType)
        {

            for (int i = 0; i < s_repeat; i++)
            {
                DeviceClient deviceClient = testDevice.CreateDeviceClient(transportType);
                using (deviceClient)
                {
                    await Task.WhenAll(SafeOpenAsync(deviceClient), SafeCloseAsync(deviceClient)).ConfigureAwait(false);
                }
            }

            _log.WriteLine($"Device {testDevice.Id} with {transportType} has been open and close {s_repeat} times.");
            _log.WriteLine("Check TCP connection to verify there is no connection leak.");
            _log.WriteLine("netstat -na | find \"[Your Hub IP]\" | find \"ESTABLISHED\"");
            await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }

        private static async Task SafeOpenAsync(DeviceClient deviceClient)
        {
            try
            {
                await deviceClient.OpenAsync().ConfigureAwait(false);
            }
            catch(Exception e)
            {
                _log.WriteLine($"OpenAsync faild: {e}");
            }
        }

        private static async Task SafeCloseAsync(DeviceClient deviceClient)
        {
            try
            {
                await deviceClient.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log.WriteLine($"CloseAsync faild: {e}");
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}
