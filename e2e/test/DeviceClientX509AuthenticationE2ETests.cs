using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics.Tracing;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.E2ETests
{
    [TestClass]
    [TestCategory("IoTHub-E2E")]
    public class DeviceClientX509AuthenticationE2ETests : IDisposable
    {
        private static readonly TestLogging _log = TestLogging.GetInstance();
        private readonly ConsoleEventListener _listener;
        private readonly string _hostName;

        public DeviceClientX509AuthenticationE2ETests()
        {
            _listener = TestConfig.StartEventListener();
            _hostName = TestDevice.GetHostName(Configuration.IoTHub.ConnectionString);
        }

        #region manual test: check connection leaking on open failure
        [TestMethod]
        public async Task X509_InvalidDeviceId_Amqp()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Amqp).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Amqp_Tcp()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Amqp_Tcp_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Amqp_WebSocket()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Amqp_WebSocket_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Mqtt()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Mqtt).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Mqtt_Tcp()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Mqtt_Tcp_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Mqtt_WebSocket()
        {
            await X509InvalidDeviceIdOpenAsyncTest(Client.TransportType.Mqtt_WebSocket_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Amqp()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Amqp).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Amqp_TCP()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Amqp_Tcp_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Amqp_WebSocket()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Amqp_WebSocket_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Mqtt()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Mqtt).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Mqtt_Tcp()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Mqtt_Tcp_Only).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task X509_InvalidDeviceId_Twice_Mqtt_WebSocket()
        {
            await X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType.Mqtt_WebSocket_Only).ConfigureAwait(false);
        }
        #endregion

        private async Task X509InvalidDeviceIdOpenAsyncTest(Client.TransportType transportType)
        {
            var deviceClient = CreateDeviceClientWithInvalidId(transportType);
            using (deviceClient)
            {
                try
                {
                    await deviceClient.OpenAsync().ConfigureAwait(false);
                    Assert.Fail("Should throw UnauthorizedException but didn't.");
                }
                catch (UnauthorizedException)
                {
                    // It should always throw UnauthorizedException
                }

                _log.WriteLine($"Invaid device with {transportType} open failed.");
                _log.WriteLine("Check TCP connection to verify there is no connection leak.");
                _log.WriteLine("netstat -na | find \"[Your Hub IP]\" | find \"ESTABLISHED\"");
                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
        }

        private async Task X509InvalidDeviceIdOpenAsyncTwiceTest(Client.TransportType transportType)
        {
            var deviceClient = CreateDeviceClientWithInvalidId(transportType);
            using (deviceClient)
            {
                for (int i = 0; i < 2; i++)
                {
                    try
                    {
                        await deviceClient.OpenAsync().ConfigureAwait(false);
                        Assert.Fail("Should throw UnauthorizedException but didn't.");
                    }
                    catch (UnauthorizedException)
                    {
                        // It should always throw UnauthorizedException
                    }
                }

                _log.WriteLine($"Invaid device with {transportType} open failed twice.");
                _log.WriteLine("Check TCP connection to verify there is no connection leak.");
                _log.WriteLine("netstat -na | find \"[Your Hub IP]\" | find \"ESTABLISHED\"");
                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
        }

        private DeviceClient CreateDeviceClientWithInvalidId(Client.TransportType transportType)
        {
            string deviceName = $"DEVICE_NOT_EXIST_{Guid.NewGuid()}";
            var auth = new DeviceAuthenticationWithX509Certificate(deviceName, Configuration.IoTHub.GetCertificateWithPrivateKey());
            return DeviceClient.Create(_hostName, auth, transportType);
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
