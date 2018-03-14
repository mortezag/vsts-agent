using ContentStoreInterfaces.Hashing;
using ContentStoreInterfaces.Utils;
using Microsoft.VisualStudio.Services.ArtifactServices.App.Shared;
using Microsoft.VisualStudio.Services.BlobStore.Common;
using Microsoft.VisualStudio.Services.BlobStore.WebApi;
using Microsoft.VisualStudio.Services.Content.Common;
using Microsoft.VisualStudio.Services.Content.Common.Tracing;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.VisualStudio.Services.Agent.Worker.Build
{
    public class DedupStorePublisher
    {
        private readonly DedupStoreClient client;
        private readonly IAppTraceSource tracer;
        private readonly TimeSpan DefaultKeepUntilDuratoin = TimeSpan.FromDays(2);

        public DedupStorePublisher(DedupStoreClient client)
        {
            this.client = client;
            this.tracer = new NoopAppTraceSource();
        }

        public async Task<PublishResult> PublishAsync(string sourceDirectory, CancellationToken cancellationToken)
        {
            var hashes = new List<FileBlobDescriptor>();
            await new PrecomputedHashesGenerator(tracer).PaginateAndProcessFiles(
                sourceDirectory: sourceDirectory,
                filePaths: null,
                chunkDedup: true,
                includeEmptyDirectories: true,
                cancellationToken: cancellationToken,
                hashCompleteCallback: (hash) => hashes.Add(hash));

            var nodes = hashes.Select(b => b.Node).ToList();
            nodes.Sort((n1, n2) => ByteArrayComparer.Instance.Compare(n1.Hash, n2.Hash));
            while (nodes.Count > 1)
            {
                nodes = nodes.GetPages(DedupNode.MaxDirectChildrenPerNode).Select(children => new DedupNode(children)).ToList();
            }

            DedupNode root = nodes.Single();
            Dictionary<DedupIdentifier, string> filePaths = new Dictionary<DedupIdentifier, string>();
            foreach (var hash in hashes)
            {
                filePaths[hash.Node.GetDedupId()] = hash.AbsolutePath;
            }

            var manifestFileName = $"{nameof(DedupStorePublisher)}.{Path.GetRandomFileName()}.manifest";
            await GenerateManifestAsync(hashes, Path.Combine(Path.GetTempPath(), manifestFileName), cancellationToken);
            var manifest = await FileBlobDescriptor.CalculateAsync(
                rootDirectory: Path.GetTempPath(),
                chunkDedup: true,
                relativePath: manifestFileName,
                cancellationToken: cancellationToken);

            filePaths[manifest.Node.GetDedupId()] = manifest.AbsolutePath;
            DedupNode topNode = new DedupNode(new[] { root, manifest.Node });
            KeepUntilBlobReference keepUntil = new KeepUntilBlobReference(DateTime.UtcNow.Add(DefaultKeepUntilDuratoin));

            var dedupUploadSession = new DedupStoreClient.UploadSession(client, keepUntil, tracer, filePaths);
            await dedupUploadSession.UploadAsync(root, cancellationToken);

            var proofNodes = ProofHelper.CreateProofNodes(
                        dedupUploadSession.AllNodes,
                        dedupUploadSession.ParentLookup,
                        hashes.Select(h => h.Node.GetDedupId()).Distinct());

            return new PublishResult(manifest.Node.GetDedupId(), root.GetDedupId(), proofNodes);
        }

        private static async Task GenerateManifestAsync(IEnumerable<FileBlobDescriptor> hashes, string manifestFilePath, CancellationToken cancellationToken)
        {
            List<Item> items = new List<Item>();
            foreach (var hash in hashes)
            {
                var item = new Item(
                    path: $"/{hash.RelativePath}",
                    blob: new Blob(
                        id: hash.BlobIdentifier.ValueString,
                        size: (ulong)hash.FileSize.Value)
                );

                items.Add(item);
            }

            var content = Content.Common.JsonSerializer.Serialize(items);
            await File.WriteAllTextAsync(manifestFilePath, content, cancellationToken);
        }
    }

    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public sealed class Item
    {
        public Item(string path, Blob blob)
        {
            Path = path;
            Blob = blob;
        }

        [JsonProperty(PropertyName = "path", Required = Required.Always)]
        public readonly string Path;

        [JsonProperty(PropertyName = "blob", Required = Required.Always)]
        public readonly Blob Blob;
    }

    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public sealed class Blob
    {
        public Blob(string id, ulong size)
        {
            Id = id;
            Size = size;
        }

        [JsonProperty(PropertyName = "id", Required = Required.Always)]
        public readonly string Id;

        [JsonProperty(PropertyName = "size", Required = Required.Always)]
        public readonly ulong Size;
    }

    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public sealed class PublishResult
    {
        [JsonProperty(PropertyName = "manifestId", Required = Required.Always)]
        public readonly DedupIdentifier ManifestId;

        [JsonProperty(PropertyName = "rootId", Required = Required.Always)]
        public readonly DedupIdentifier RootId;

        public readonly ISet<DedupNode> ProofNodes;

        public PublishResult(DedupIdentifier manifestId, DedupIdentifier root, ISet<DedupNode> proofNodes)
        {
            this.ManifestId = manifestId;
            this.RootId = root;
            this.ProofNodes = proofNodes;
        }
    }
}
