﻿using Microsoft.TeamFoundation.Core.WebApi;
using Agent.Sdk;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.TeamFoundation.Build.WebApi;
using Microsoft.VisualStudio.Services.WebApi;
using Microsoft.VisualStudio.Services.Agent.Util;

namespace Agent.Plugins.Drop
{
    // A client wrapper interacting with TFS/Build's Artifact API
    public class BuildServiceClient
    {
        private readonly BuildHttpClient _buildHttpClient;

        public BuildServiceClient(VssConnection connection)
        {
            ArgUtil.NotNull(connection, nameof(connection));
            _buildHttpClient = connection.GetClient<BuildHttpClient>();
        }

        // Associate the specified artifact with a build, along with custom data.
        public async Task<BuildArtifact> AssociateArtifact(
            Guid projectId,
            int buildId,
            string name,
            string type,
            string data,
            Dictionary<string, string> propertiesDictionary,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            BuildArtifact artifact = new BuildArtifact()
            {
                Name = name,
                Resource = new ArtifactResource()
                {
                    Data = data,
                    Type = type,
                    Properties = propertiesDictionary
                }
            };

            return await _buildHttpClient.CreateArtifactAsync(artifact, projectId, buildId, cancellationToken: cancellationToken);
        }

        // Get named artifact from a build
        public async Task<BuildArtifact> GetArtifact(
            Guid projectId,
            int buildId,
            string name,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await _buildHttpClient.GetArtifactAsync(projectId, buildId, name, null, cancellationToken);
        }
    }
}
