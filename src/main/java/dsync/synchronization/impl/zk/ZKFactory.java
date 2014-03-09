/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dsync.synchronization.impl.zk;

import dsync.synchronization.CyclicBarrier;
import dsync.synchronization.DoubleBarrier;
import dsync.synchronization.InitializationException;
import dsync.synchronization.Lock;
import dsync.synchronization.LockFactory;
import dsync.synchronization.ReadWriteLock;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKFactory implements LockFactory {

    private static ZKFactory factory;
    private ZKConfiguration config;
    private ZooKeeper zookeeper;
    private FactoryWatcher watcher;

    private ZKFactory(ZKConfiguration config) throws IOException {
        this.config = config;
        init();
    }

    public static synchronized ZKFactory getInstance(ZKConfiguration config) throws IOException {
        if (factory == null) {
            factory = new ZKFactory(config);
        }

        return factory;
    }
/*
    public static ZKFactory newInstance(ZKConfiguration config) throws IOException {
        return new ZKFactory(config);
    }*/

    public Lock getReentrantLock(Object lockAttribute) {

        Lock lock = new ReentrantLock(zookeeper, config.getAcls(), (String) lockAttribute);
        return lock;

    }

    public ReadWriteLock getReentrantReadWriteLock(Object lockAttribute) {
        ReadWriteLock lock = new ReentrantReadWriteLock(zookeeper, config.getAcls(),
                                                        (String) lockAttribute);
        return lock;
    }

    public CyclicBarrier getCyclicBarrier(Object lockAttribute, int parties)
            throws InitializationException {
        CyclicBarrier barrier = new ZKBarrier(zookeeper, config.getAcls(), (String) lockAttribute,
                                              parties);
        return barrier;
    }

    public CyclicBarrier getCyclicBarrier(Object lockAttribute, int parties, Runnable barrierAction)
            throws InitializationException {
        CyclicBarrier barrier = new ZKBarrier(zookeeper, config.getAcls(), (String) lockAttribute,
                                              parties, barrierAction);
        return barrier;
    }

    public DoubleBarrier getDoubleBarrier(Object lockAttribute, int parties)
            throws InitializationException {
        DoubleBarrier barrier = new ZKDoubleBarrier(zookeeper, config.getAcls(), (String) lockAttribute,
                                                    parties);
        return barrier;

    }

    private void init() throws IOException {
        watcher = new FactoryWatcher();

        List<ACL> acls = config.getAcls();

        if (acls == null) {

            acls = new ArrayList<ACL>();

            for (ACL ids_acl : ZooDefs.Ids.OPEN_ACL_UNSAFE) {
                acls.add(ids_acl);
            }

            config.setAcls(acls);
        }

        zookeeper = new ZooKeeper(config.getConnectionString(), config.getTimeout(), watcher);

    }

    private class FactoryWatcher implements Watcher {

        public void process(WatchedEvent watchedEvent) {
            System.out.println("Timed out..");
        }
    }
}
