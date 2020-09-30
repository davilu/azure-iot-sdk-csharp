// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Client.Extensions;
using Microsoft.Azure.Devices.Client.Transport.Amqp;
using Microsoft.Azure.Devices.Shared;

namespace Microsoft.Azure.Devices.Client.Transport.AmqpIoT
{
    internal class AmqpIoTConnection
    {
        public event EventHandler Closed;
        private readonly AmqpConnection _amqpConnection;
        private readonly AmqpIoTCbsLink _amqpIoTCbsLink;

        internal AmqpIoTConnection(AmqpConnection amqpConnection)
        {
            _amqpConnection = amqpConnection;
            _amqpIoTCbsLink = new AmqpIoTCbsLink(new AmqpCbsLink(amqpConnection));
        }

        internal AmqpIoTCbsLink GetCbsLink()
        {
            return _amqpIoTCbsLink;
        }

        internal void AmqpConnectionClosed(object sender, EventArgs e)
        {
            if (Logger.IsEnabled) Logger.Enter(this, $"{nameof(AmqpConnectionClosed)}");
            Closed?.Invoke(this, e);
            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(AmqpConnectionClosed)}");
        }

        internal async Task<AmqpIoTSession> OpenSessionAsync(TimeSpan timeout)
        {
            if (_amqpConnection.IsClosing())
            {
                if (Logger.IsEnabled) Logger.Error(this, "AMQP connection is disconnected.", $"{nameof(OpenSessionAsync)}");
                throw new IotHubCommunicationException("Amqp connection is disconnected.");
            }

            if (Logger.IsEnabled) Logger.Enter(this, timeout, $"{nameof(OpenSessionAsync)}");
            AmqpSessionSettings amqpSessionSettings = new AmqpSessionSettings()
            {
                Properties = new Fields()
            };

            try
            {
                var amqpSession = new AmqpSession(_amqpConnection, amqpSessionSettings, AmqpIoTLinkFactory.GetInstance());
                _amqpConnection.AddSession(amqpSession, new ushort?());
                await amqpSession.OpenAsync(timeout).ConfigureAwait(false);
                return new AmqpIoTSession(amqpSession);
            }
            catch(Exception e) when (!e.IsFatal())
            {
                if (Logger.IsEnabled) Logger.Error(this, e, $"{nameof(OpenSessionAsync)}");
                Exception ex = AmqpIoTExceptionAdapter.ConvertToIoTHubException(e, _amqpConnection);
                if (ReferenceEquals(e, ex))
                {
                    throw;
                }
                else
                {
                    if (ex is AmqpIoTResourceException)
                    {
                        _amqpConnection.SafeClose();
                        throw new IotHubCommunicationException(ex.Message, ex);
                    }
                    throw ex;
                }
            }
            finally
            {
                if (Logger.IsEnabled) Logger.Exit(this, timeout, $"{nameof(OpenSessionAsync)}");
            }
        }

        internal async Task<IAmqpAuthenticationRefresher> CreateRefresherAsync(DeviceIdentity deviceIdentity, TimeSpan timeout)
        {
            if (_amqpConnection.IsClosing())
            {
                if (Logger.IsEnabled) Logger.Error(this, "AMQP connection is disconnected.", $"{nameof(CreateRefresherAsync)}");
                throw new IotHubCommunicationException("Amqp connection is disconnected.");
            }

            if (Logger.IsEnabled) Logger.Enter(this, deviceIdentity, timeout, $"{nameof(CreateRefresherAsync)}");
            try
            {
                IAmqpAuthenticationRefresher amqpAuthenticator = new AmqpAuthenticationRefresher(deviceIdentity, _amqpIoTCbsLink);
                await amqpAuthenticator.InitLoopAsync(timeout).ConfigureAwait(false);
                return amqpAuthenticator;
            }
            catch (Exception e) when (!e.IsFatal())
            {
                if (Logger.IsEnabled) Logger.Error(this, e, $"{nameof(CreateRefresherAsync)}");
                Exception ex = AmqpIoTExceptionAdapter.ConvertToIoTHubException(e, _amqpConnection);
                if (ReferenceEquals(e, ex))
                {
                    throw;
                }
                else
                {
                    throw ex;
                }
            }
            finally
            {
                if (Logger.IsEnabled) Logger.Enter(this, deviceIdentity, timeout, $"{nameof(CreateRefresherAsync)}");
            }
        }

        internal void SafeClose()
        {
            _amqpConnection.SafeClose();
        }

        internal bool IsClosing()
        {
            return _amqpConnection.IsClosing();
        }
    }
}
