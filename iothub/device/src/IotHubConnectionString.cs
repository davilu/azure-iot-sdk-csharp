﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client
{
    using System;
    using System.Net;

#if !NETMF
    using System.Threading.Tasks;
#endif

    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Azure.Devices.Shared;
    using System.Diagnostics;

    internal sealed partial class IotHubConnectionString : IAuthorizationProvider
    {
        const string UserSeparator = "@";
        const int DefaultSecurePort = 5671;

        public IotHubConnectionString(IotHubConnectionStringBuilder builder)
        {
            if (builder == null)
            {
                throw new ArgumentNullException("builder");
            }

            this.Audience = builder.HostName;
            this.HostName = builder.GatewayHostName == null || builder.GatewayHostName == "" ? builder.HostName : builder.GatewayHostName;
            this.SharedAccessKeyName = builder.SharedAccessKeyName;
            this.SharedAccessKey = builder.SharedAccessKey;
            this.SharedAccessSignature = builder.SharedAccessSignature;
            this.IotHubName = builder.IotHubName;
            this.DeviceId = builder.DeviceId;
            this.ModuleId = builder.ModuleId;

#if NETSTANDARD1_3
            this.HttpsEndpoint = new UriBuilder("https", this.HostName).Uri;
#elif !NETMF
            this.HttpsEndpoint = new UriBuilder(Uri.UriSchemeHttps, this.HostName).Uri;
#elif NETMF
            this.HttpsEndpoint = new Uri("https://" + this.HostName);
#endif

#if !NETMF
            this.AmqpEndpoint = new UriBuilder(CommonConstants.AmqpsScheme, this.HostName, DefaultSecurePort).Uri;

            if (builder.AuthenticationMethod is AuthenticationWithTokenRefresh)
            {
                this.TokenRefresher = (AuthenticationWithTokenRefresh)builder.AuthenticationMethod;
                if (Logger.IsEnabled) Logger.Associate(this, TokenRefresher, nameof(TokenRefresher));
                Debug.Assert(TokenRefresher != null);
            }
            else if (!string.IsNullOrEmpty(this.SharedAccessKey))
            {
                if (this.ModuleId.IsNullOrWhiteSpace())
                {
                    this.TokenRefresher = new DeviceAuthenticationWithSakRefresh(this.DeviceId, this) as AuthenticationWithTokenRefresh;
                }
                else
                {
                    this.TokenRefresher = new ModuleAuthenticationWithSakRefresh(this.DeviceId, this.ModuleId, this) as AuthenticationWithTokenRefresh;
                }

                if (Logger.IsEnabled) Logger.Associate(this, TokenRefresher, nameof(TokenRefresher));
                Debug.Assert(TokenRefresher != null);
            }
#endif
        }

        public string IotHubName
        {
            get;
            private set;
        }

        public string DeviceId
        {
            get;
            private set;
        }

        public string ModuleId
        {
            get;
            private set;
        }

        public string HostName
        {
            get;
            private set;
        }

        public Uri HttpsEndpoint
        {
            get;
            private set;
        }

#if !NETMF
        public Uri AmqpEndpoint
        {
            get;
            private set;
        }
#endif

        public string Audience
        {
            get;
            private set;
        }

        public string SharedAccessKeyName
        {
            get;
            private set;
        }

        public string SharedAccessKey
        {
            get;
            private set;
        }

        public string SharedAccessSignature
        {
            get;
            private set;
        }
    }
}
