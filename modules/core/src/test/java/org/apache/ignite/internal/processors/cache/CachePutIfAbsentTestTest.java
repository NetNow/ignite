/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jsr166.ConcurrentHashMap8;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CachePutIfAbsentTestTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final boolean FAST = false;

    /** */
    private static Map<Integer, Integer> storeMap = new ConcurrentHashMap8<>();

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 3;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        // No store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, false, false));

        // Store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, true, false));

        // No store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, true));

        // Store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, true));

        // Swap and offheap enabled.
        for (GridTestUtils.TestMemoryMode memMode : GridTestUtils.TestMemoryMode.values()) {
            CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);

            GridTestUtils.setMemoryMode(null, ccfg, memMode, 1, 64);

            ccfgs.add(ccfg);
        }

        return ccfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param storeEnabled If {@code true} adds cache store.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean storeEnabled,
        boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (storeEnabled) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setWriteThrough(true);
            ccfg.setReadThrough(true);
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictGetAndPutIfAbsent() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                Integer key = getNoneLocalKey(ignite0, cache);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Object old = cache.getAndPutIfAbsent(key, 2);

                    assertNull(old);

                    tx.commit();
                }

                Object val = cache.get(key);

                info("Val: " + val);

                assertEquals(2, val);

                Object old = cache.getAndPutIfAbsent(key, 3);

                assertEquals(2, old);

                val = cache.get(key);

                info("Val: " + val);

                assertEquals(2, val);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    old = cache.getAndPutIfAbsent(key, 3);

                    info("Val: " + old);

                    assertEquals(2, old);

                    val = cache.get(key);

                    info("Val: " + val);

//                    assertEquals(val, old);

                    tx.commit();
                }

                info("Val: " + cache.get(key));

                assertEquals(val, old);
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     */
    private Integer getNoneLocalKey(Ignite ignite, IgniteCache cache) {
        for (int i =0; i<100; i++)
            if (!isKeyLocal(ignite, cache, i))
                return i;

        fail("Can't find none local key.");

        return null;
    }

    /**
     * Checks, whether the key is stored locally.
     * @param ignite Ignite 0.
     * @param cache Cache.
     * @param key Key.
     */
    private boolean isKeyLocal(Ignite ignite, IgniteCache cache, Integer key) {
        return ((TcpDiscoveryNode)((ArrayList)ignite.affinity(cache.getName())
            .mapKeyToPrimaryAndBackups(ignite.affinity(cache.getName()).affinityKey(key)))
            .get(0)).id().equals(((IgniteKernal)ignite).getLocalNodeId());
    }

    /**
     * @param cacheName Cache name.
     */
    private void destroyCache(String cacheName) {
        storeMap.clear();

        for (Ignite ignite : G.allGrids()) {
            try {
                ignite.destroyCache(cacheName);
            }
            catch (IgniteException ignore) {
                // No-op.
            }

            GridTestSwapSpaceSpi spi = (GridTestSwapSpaceSpi)ignite.configuration().getSwapSpaceSpi();

            spi.clearAll();
        }
    }


    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return new CacheStoreAdapter<Integer, Integer>() {
                @Override public Integer load(Integer key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) {
                    storeMap.remove(key);
                }
            };
        }
    }
}
