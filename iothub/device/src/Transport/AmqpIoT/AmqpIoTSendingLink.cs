// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Client.Extensions;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;

namespace Microsoft.Azure.Devices.Client.Transport.AmqpIoT
{
    internal class AmqpIoTSendingLink
    {
        public event EventHandler Closed;

        private readonly SendingAmqpLink _sendingAmqpLink;

        public AmqpIoTSendingLink(SendingAmqpLink sendingAmqpLink)
        {
            _sendingAmqpLink = sendingAmqpLink;
            _sendingAmqpLink.Closed += SendingAmqpLinkClosed;
        }

        private void SendingAmqpLinkClosed(object sender, EventArgs e)
        {
            if (Logger.IsEnabled) Logger.Enter(this, sender, $"{nameof(SendingAmqpLinkClosed)}");
            Closed?.Invoke(this, e);
            if (Logger.IsEnabled) Logger.Exit(this, sender, $"{nameof(SendingAmqpLinkClosed)}");
        }

        internal Task CloseAsync(TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, timeout, $"{nameof(CloseAsync)}");
            try
            {
                return _sendingAmqpLink.CloseAsync(timeout);
            }
            finally
            {
                if (Logger.IsEnabled) Logger.Exit(this, timeout, $"{nameof(CloseAsync)}");
            }
        }

        internal void SafeClose()
        {
            if (Logger.IsEnabled) Logger.Enter(this, $"{nameof(SafeClose)}");
            _sendingAmqpLink.SafeClose();
            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(SafeClose)}");
        }

        internal bool IsClosing()
        {
            return _sendingAmqpLink.IsClosing();
        }

        #region Telemetry handling

        internal async Task<AmqpIoTOutcome> SendMessageAsync(Message message, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, message, timeout, $"{nameof(SendMessageAsync)}");

            try
            {
                AmqpMessage amqpMessage = AmqpIoTMessageConverter.MessageToAmqpMessage(message);
                Outcome outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);
                return new AmqpIoTOutcome(outcome);
            }
            catch (Exception e)
            {
                if (Logger.IsEnabled) Logger.Error(this, e, $"{nameof(SendMessageAsync)}");
                throw;
            }
            finally
            { 
                if (Logger.IsEnabled) Logger.Exit(this, message, timeout, $"{nameof(SendMessageAsync)}");
            }

        }

        internal async Task<AmqpIoTOutcome> SendMessagesAsync(IEnumerable<Message> messages, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, timeout, $"{nameof(SendMessagesAsync)}");

            // List to hold messages in Amqp friendly format
            var messageList = new List<Data>();

            foreach (Message message in messages)
            {
                using (AmqpMessage amqpMessage = AmqpIoTMessageConverter.MessageToAmqpMessage(message))
                {
                    var data = new Data()
                    {
                        Value = AmqpIoTMessageConverter.ReadStream(amqpMessage.ToStream())
                    };
                    messageList.Add(data);
                }
            }

            try
            {
                Outcome outcome;
                using (AmqpMessage amqpMessage = AmqpMessage.Create(messageList))
                {
                    amqpMessage.MessageFormat = AmqpConstants.AmqpBatchedMessageFormat;
                    outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);
                }

                AmqpIoTOutcome amqpIoTOutcome = new AmqpIoTOutcome(outcome);
                if (amqpIoTOutcome != null)
                {
                    amqpIoTOutcome.ThrowIfNotAccepted();
                }

                return amqpIoTOutcome;
            }
            catch (Exception e)
            {
                if (Logger.IsEnabled) Logger.Error(this, e, $"{nameof(SendMessagesAsync)}");
                throw;
            }
            finally
            {
                if (Logger.IsEnabled) Logger.Exit(this, timeout, $"{nameof(SendMessagesAsync)}");
            }
        }

        private async Task<Outcome> SendAmqpMessageAsync(AmqpMessage amqpMessage, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, timeout, $"{nameof(SendAmqpMessageAsync)}");

            try
            {
                return await _sendingAmqpLink.SendMessageAsync(
                    amqpMessage,
                    new ArraySegment<byte>(Guid.NewGuid().ToByteArray()),
                    AmqpConstants.NullBinary,
                    timeout).ConfigureAwait(false);
            }
            catch (Exception e) when (!e.IsFatal())
            {
                if (Logger.IsEnabled) Logger.Error(this, e, $"{nameof(SendAmqpMessageAsync)}");

                Exception ex = AmqpIoTExceptionAdapter.ConvertToIoTHubException(e, _sendingAmqpLink);
                if (ReferenceEquals(e, ex))
                {
                    throw;
                }
                else
                {
                    if (ex is AmqpIoTResourceException)
                    {
                        _sendingAmqpLink.SafeClose();
                        throw new IotHubCommunicationException(ex.Message, ex);
                    }
                    throw ex;
                }
            }
            finally
            {
                if (Logger.IsEnabled) Logger.Exit(this, timeout, $"{nameof(SendAmqpMessageAsync)}");
            }
        }

        #endregion Telemetry handling

        #region Method handling

        internal async Task<AmqpIoTOutcome> SendMethodResponseAsync(MethodResponseInternal methodResponse, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, methodResponse, $"{nameof(SendMethodResponseAsync)}");

            AmqpMessage amqpMessage = AmqpIoTMessageConverter.ConvertMethodResponseInternalToAmqpMessage(methodResponse);
            AmqpIoTMessageConverter.PopulateAmqpMessageFromMethodResponse(amqpMessage, methodResponse);

            Outcome outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);

            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(SendMethodResponseAsync)}");

            return new AmqpIoTOutcome(outcome);
        }

        #endregion Method handling

        #region Twin handling

        internal async Task<AmqpIoTOutcome> SendTwinGetMessageAsync(string correlationId, TwinCollection reportedProperties, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, $"{nameof(SendTwinGetMessageAsync)}");

            AmqpMessage amqpMessage = AmqpMessage.Create();
            amqpMessage.Properties.CorrelationId = correlationId;
            amqpMessage.MessageAnnotations.Map["operation"] = "GET";

            Outcome outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);

            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(SendTwinGetMessageAsync)}");

            return new AmqpIoTOutcome(outcome);
        }

        internal async Task<AmqpIoTOutcome> SendTwinPatchMessageAsync(string correlationId, TwinCollection reportedProperties, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, $"{nameof(SendTwinPatchMessageAsync)}");

            var body = JsonConvert.SerializeObject(reportedProperties);
            var bodyStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(body));

            AmqpMessage amqpMessage = AmqpMessage.Create(bodyStream, true);
            amqpMessage.Properties.CorrelationId = correlationId;
            amqpMessage.MessageAnnotations.Map["operation"] = "PATCH";
            amqpMessage.MessageAnnotations.Map["resource"] = "/properties/reported";
            amqpMessage.MessageAnnotations.Map["version"] = null;

            Outcome outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);

            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(SendTwinPatchMessageAsync)}");

            return new AmqpIoTOutcome(outcome);
        }

        internal async Task<AmqpIoTOutcome> SubscribeToDesiredPropertiesAsync(string correlationId, TimeSpan timeout)
        {
            if (Logger.IsEnabled) Logger.Enter(this, $"{nameof(SubscribeToDesiredPropertiesAsync)}");

            AmqpMessage amqpMessage = AmqpMessage.Create();
            amqpMessage.Properties.CorrelationId = correlationId;
            amqpMessage.MessageAnnotations.Map["operation"] = "PUT";
            amqpMessage.MessageAnnotations.Map["resource"] = "/notifications/twin/properties/desired";
            amqpMessage.MessageAnnotations.Map["version"] = null;

            Outcome outcome = await SendAmqpMessageAsync(amqpMessage, timeout).ConfigureAwait(false);

            if (Logger.IsEnabled) Logger.Exit(this, $"{nameof(SubscribeToDesiredPropertiesAsync)}");

            return new AmqpIoTOutcome(outcome);
        }

        #endregion Twin handling
    }
}
