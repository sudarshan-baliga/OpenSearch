/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Objects;

/**
 * This class has the common code used in TransportNodesBatchListGatewayStartedShards and TransportNodesListGatewayStartedShards
 *
 * @opensearch.internal
 */
public class TransportNodesGatewayStartedShardHelper {

    /**
     * Class for storing the information about the shards fetched on the node.
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShardInfo {
        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;
        private final ReplicationCheckpoint replicationCheckpoint;

        public NodeGatewayStartedShardInfo(StreamInput in) throws IOException {
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
            if (in.getVersion().onOrAfter(Version.V_2_3_0) && in.readBoolean()) {
                replicationCheckpoint = new ReplicationCheckpoint(in);
            } else {
                replicationCheckpoint = null;
            }
        }

        public NodeGatewayStartedShardInfo(String allocationId, boolean primary, ReplicationCheckpoint replicationCheckpoint) {
            this(allocationId, primary, replicationCheckpoint, null);
        }

        public NodeGatewayStartedShardInfo(String allocationId, boolean primary, ReplicationCheckpoint replicationCheckpoint, Exception storeException) {
            this.allocationId = allocationId;
            this.primary = primary;
            this.replicationCheckpoint = replicationCheckpoint;
            this.storeException = storeException;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public ReplicationCheckpoint replicationCheckpoint() {
            return this.replicationCheckpoint;
        }

        public Exception storeException() {
            return this.storeException;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(allocationId);
            out.writeBoolean(primary);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
            if (out.getVersion().onOrAfter(Version.V_2_3_0)) {
                if (replicationCheckpoint != null) {
                    out.writeBoolean(true);
                    replicationCheckpoint.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NodeGatewayStartedShardInfo that = (NodeGatewayStartedShardInfo) o;

            return primary == that.primary && Objects.equals(allocationId, that.allocationId) && Objects.equals(storeException, that.storeException) && Objects.equals(replicationCheckpoint, that.replicationCheckpoint);
        }

        @Override
        public int hashCode() {
            int result = (allocationId != null ? allocationId.hashCode() : 0);
            result = 31 * result + (primary ? 1 : 0);
            result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
            result = 31 * result + (replicationCheckpoint != null ? replicationCheckpoint.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("NodeGatewayStartedShards[").append("allocationId=").append(allocationId).append(",primary=").append(primary);
            if (storeException != null) {
                buf.append(",storeException=").append(storeException);
            }
            if (replicationCheckpoint != null) {
                buf.append(",ReplicationCheckpoint=").append(replicationCheckpoint.toString());
            }
            buf.append("]");
            return buf.toString();
        }
    }

    /**
     * Helper function for getting the data path of the shard that is used to look up information for this shard.
     * If the dataPathInRequest passed to the method is not empty then same is returned. Else the custom data path is returned
     * from the indexSettings fetched from the cluster state metadata for the specified shard.
     *
     * @param logger
     * @param shardId
     * @param dataPathInRequest
     * @param settings
     * @param clusterService
     * @return String
     */
    public static String getCustomDataPathForShard(
        Logger logger,
        ShardId shardId,
        String dataPathInRequest,
        Settings settings,
        ClusterService clusterService
    ) {
        if (dataPathInRequest != null) return dataPathInRequest;
        // TODO: Fallback for BWC with older OpenSearch versions.
        // Remove once request.getCustomDataPath() always returns non-null
        final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
        if (metadata != null) {
            return new IndexSettings(metadata, settings).customDataPath();
        } else {
            logger.trace("{} node doesn't have meta data for the requests index", shardId);
            throw new OpenSearchException("node doesn't have meta data for index " + shardId.getIndex());
        }
    }

    /**
     * Helper function for checking if the shard file exists and is not corrupted. We return the specific exception if
     * the shard file is corrupted. else null value is returned.
     *
     * @param logger
     * @param nodeEnv
     * @param shardId
     * @param shardStateMetadata
     * @param customDataPath
     * @return Exception
     */
    public static Exception checkShardCorruption(
        Logger logger,
        NodeEnvironment nodeEnv,
        ShardId shardId,
        ShardStateMetadata shardStateMetadata,
        String customDataPath
    ) {
        ShardPath shardPath = null;
        try {
            shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
            if (shardPath == null) {
                throw new IllegalStateException(shardId + " no shard path found");
            }
            Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
        } catch (Exception exception) {
            final ShardPath finalShardPath = shardPath;
            logger.trace(
                () -> new ParameterizedMessage(
                    "{} can't open index for shard [{}] in path [{}]",
                    shardId,
                    shardStateMetadata,
                    (finalShardPath != null) ? finalShardPath.resolveIndex() : ""
                ),
                exception
            );
            return exception;
        }
        return null;
    }
    public static NodeGatewayStartedShardInfo getShardInfoOnLocalNode(
        Logger logger,
        final ShardId shardId,
        NamedXContentRegistry namedXContentRegistry,
        NodeEnvironment nodeEnv,
        IndicesService indicesService,
        String shardDataPath,
        Settings settings,
        ClusterService clusterService
    ) throws IOException {
        logger.trace("{} loading local shard state info", shardId);
        ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
            logger,
            namedXContentRegistry,
            nodeEnv.availableShardPaths(shardId)
        );
        if (shardStateMetadata != null) {
            if (indicesService.getShardOrNull(shardId) == null && shardStateMetadata.indexDataLocation == ShardStateMetadata.IndexDataLocation.LOCAL) {
                final String customDataPath = getCustomDataPathForShard(
                    logger,
                    shardId,
                    shardDataPath,
                    settings,
                    clusterService
                );
                // we don't have an open shard on the store, validate the files on disk are openable
                Exception shardCorruptionException = checkShardCorruption(
                    logger,
                    nodeEnv,
                    shardId,
                    shardStateMetadata,
                    customDataPath
                );
                if (shardCorruptionException != null) {
                    String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                    return new NodeGatewayStartedShardInfo(
                        allocationId,
                        shardStateMetadata.primary,
                        null,
                        shardCorruptionException
                    );
                }
            }
            logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
            String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
            final IndexShard shard = indicesService.getShardOrNull(shardId);
            return new NodeGatewayStartedShardInfo(
                allocationId,
                shardStateMetadata.primary,
                shard != null ? shard.getLatestReplicationCheckpoint() : null
            );
        }
        logger.trace("{} no local shard info found", shardId);
        return new NodeGatewayStartedShardInfo(null, false, null);
    }
}
