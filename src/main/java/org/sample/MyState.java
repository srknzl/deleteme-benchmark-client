package org.sample;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSendAllSchemasCodec;
import com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder;
import com.hazelcast.client.impl.spi.ProxyManager;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.map.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaWriter;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import static com.hazelcast.internal.nio.IOUtil.toByteArray;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

@State(Scope.Thread)
public class MyState {
    private static HazelcastClientInstanceImpl client;
    private static ClientExecutionServiceImpl taskScheduler;
    private static ClientQueryCacheContext queryCacheContext;
    private static ProxyManager proxyManager;
    private static final List<CacheConfig> cacheConfigs = new ArrayList<>();
    private static final Map<Long, Schema> schemas = new HashMap<>();
    private static final List<Map.Entry<String, byte[]>> classDefinitionList = new ArrayList<>();
    private static final int NUMBER_OF_USER_DEPLOYMENT_ENTRIES = 10;
    private static final int NUMBER_OF_COMPACT_SCHEMAS = 100;
    private static final int NUMBER_OF_QUERY_CACHES = 100;
    private static final int NUMBER_OF_MAP_PROXIES = 10000;
    private static final int NUMBER_OF_CACHES = 1000;
    private static final String MAP_PREFIX = "map-name-";
    private static final String CACHE_PREFIX = "cache-name-";
    private static final String QUERY_CACHE_NAME = "cache-name";
    public static final String SCHEMA_SERVICE_NAME = "schema-service";

    @Setup(Level.Trial)
    public void doSetup() throws IOException {
        ClientConfig clientConfig = new ClientConfig();
        String clusterIp = System.getProperty("clusterIp");
        clientConfig.getNetworkConfig().addAddress(clusterIp);
        clientConfig.getUserCodeDeploymentConfig().setEnabled(true);
        configureQueryCaches(clientConfig);
        configureICaches();
        HazelcastInstance hzInstanceClient = HazelcastClient.newHazelcastClient(clientConfig);
        client = getHazelcastClientInstanceImpl(hzInstanceClient);
        taskScheduler = (ClientExecutionServiceImpl) client.getTaskScheduler();
        queryCacheContext = client.getQueryCacheContext();
        proxyManager = client.getProxyManager();
        populateClassDefinitionForUserCodeDeployment();
        populateCompactSchemas();
        createQueryCaches();
        createProxies();
    }


    @TearDown(Level.Trial)
    public void doTearDown() {
        // destroyCaches();
        client.shutdown();
        taskScheduler.shutdown();
    }

    private static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        if (client instanceof HazelcastClientProxy) {
            return ((HazelcastClientProxy) client).client;
        } else if (client instanceof HazelcastClientInstanceImpl) {
            return ((HazelcastClientInstanceImpl) client);
        } else {
            throw new IllegalArgumentException("This method can be called only with client"
                    + " instances such as HazelcastClientProxy and HazelcastClientInstanceImpl.");
        }
    }

    private static void deployUserCode() throws ExecutionException, InterruptedException {
        ClientMessage request = ClientDeployClassesCodec.encodeRequest(classDefinitionList);
        ClientInvocation invocation = new ClientInvocation(client, request, null);
        ClientInvocationFuture future = invocation.invokeUrgent();
        future.get();
    }

    private static void sendAllSchemas() {
        ClientMessage clientMessage = ClientSendAllSchemasCodec.encodeRequest(new ArrayList<>(schemas.values()));
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, SCHEMA_SERVICE_NAME);
        invocation.invokeUrgent().joinInternal();
    }

    public void sendStateToCluster() throws ExecutionException, InterruptedException {
        CompletionService<Void> completionService = new ExecutorCompletionService<>(taskScheduler);
        List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(() -> {
            deployUserCode();
            return null;
        });
        tasks.add(() -> {
            sendAllSchemas();
            return null;
        });
        tasks.add(() -> {
            queryCacheContext.recreateAllCaches();
            return null;
        });
        tasks.add(() -> {
            proxyManager.createDistributedObjectsOnCluster();
            // Since there is no cache, we will just send invocations for each cache.
            recreateCaches();
            return null;
        });
        for (Callable<Void> task : tasks) {
            completionService.submit(task);
        }
        for (int i = 0; i < tasks.size(); i++) {
            completionService.take().get();
        }
    }

    public void sendStateToClusterMaster() throws ExecutionException, InterruptedException {
        deployUserCode();
        sendAllSchemas();
        queryCacheContext.recreateAllCaches();
        proxyManager.createDistributedObjectsOnCluster();
        // Since there is no cache, we will just send invocations for each cache.
        recreateCaches();
    }

    private static String randomString() {
        return UuidUtil.newUnsecureUuidString();
    }

    private static void createProxies() {
        for (int i = 0; i < NUMBER_OF_MAP_PROXIES; i++) {
            client.getMap(MAP_PREFIX + i);
        }
    }

    private static void recreateCaches() throws InterruptedException, ExecutionException {
        CompletionService<Void> completionService = new ExecutorCompletionService<>(client.getTaskScheduler());
        int numberOfTasks = 0;
        for (CacheConfig cacheConfig : cacheConfigs) {
            numberOfTasks++;
            completionService.submit(() -> {
                createCacheConfig(client, cacheConfig, true);
                return null;
            });
        }
        for (int i = 0; i < numberOfTasks; i++) {
            completionService.take().get();
        }
    }

    private static  <K, V> CacheConfig<K, V> createCacheConfig(HazelcastClientInstanceImpl client,
                                          CacheConfig<K, V> newCacheConfig, boolean urgent) {
        try {
            String nameWithPrefix = newCacheConfig.getNameWithPrefix();
            int partitionId = client.getClientPartitionService().getPartitionId(nameWithPrefix);

            InternalSerializationService serializationService = client.getSerializationService();
            ClientMessage request = CacheCreateConfigCodec
                    .encodeRequest(CacheConfigHolder.of(newCacheConfig, serializationService), true);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, nameWithPrefix, partitionId);
            Future<ClientMessage> future = urgent ? clientInvocation.invokeUrgent() : clientInvocation.invoke();
            final ClientMessage response = future.get();
            final CacheConfigHolder cacheConfigHolder = CacheCreateConfigCodec.decodeResponse(response);
            if (cacheConfigHolder == null) {
                return null;
            }
            return cacheConfigHolder.asCacheConfig(serializationService);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static void createQueryCaches() {
        for (int i = 0; i < NUMBER_OF_QUERY_CACHES; i++) {
            IMap<Integer, Integer> clientMap = client.getMap(MAP_PREFIX + i);
            clientMap.getQueryCache(QUERY_CACHE_NAME);
        }
    }

    private static void configureQueryCaches(ClientConfig clientConfig) {
        for (int i = 0; i < NUMBER_OF_QUERY_CACHES; i++) {
            QueryCacheConfig queryCacheConfig = new QueryCacheConfig(QUERY_CACHE_NAME);
            queryCacheConfig.getPredicateConfig().setImplementation(Predicates.greaterThan("this", 10));
            clientConfig.addQueryCacheConfig(MAP_PREFIX + i, queryCacheConfig);
        }
    }

    private static void configureICaches() {
        for (int i = 0; i < NUMBER_OF_CACHES; i++) {
            CacheConfig<Integer, Integer> cacheConfig = new CacheConfig<>(CACHE_PREFIX + i);
            cacheConfigs.add(cacheConfig);
        }
    }

    private static void populateCompactSchemas() {
        for (int i = 0; i < NUMBER_OF_COMPACT_SCHEMAS; i++) {
            SchemaWriter schemaWriter = new SchemaWriter(randomString());
            // Some diversity magic:
            if (i % 2 == 0) {
                schemaWriter.writeBoolean(randomString(), true);
                schemaWriter.writeInt8(randomString(), (byte) 1);
            }
            if (i % 3 == 0) {
                schemaWriter.writeInt8(randomString(), (byte) 1);
                schemaWriter.writeArrayOfDate(randomString(), null);
            }
            if (i % 5 == 0) {
                schemaWriter.writeString(randomString(), null);
                schemaWriter.writeDecimal(randomString(), null);
            }
            Schema schema = schemaWriter.build();
            schemas.put(schema.getSchemaId(), schema);
        }
    }

    private static void populateClassDefinitionForUserCodeDeployment() throws IOException {
        for (int i = 0; i < NUMBER_OF_USER_DEPLOYMENT_ENTRIES; i++) {
            classDefinitionList.add(new ClassDefinition());
        }
    }

    // ClassDefinition for user code deployment
    private static class ClassDefinition implements Map.Entry<String, byte[]> {

        private final String key;
        private byte[] value;

        public ClassDefinition() throws IOException {
            this.key = "org.sample.SampleClass";
            InputStream is = new FileInputStream("src/main/java/org/sample/SampleClass.class");
            this.value = toByteArray(is);
        }

        @Override
        public String getKey() {
            return this.key;
        }

        @Override
        public byte[] getValue() {
            return this.value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            byte[] oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }
}
