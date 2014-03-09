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
package dsync;

import dsync.synchronization.InitializationException;
import dsync.synchronization.impl.zk.ReentrantLock;
import dsync.synchronization.impl.zk.ReentrantReadWriteLock;
import dsync.synchronization.impl.zk.ZKBarrier;
import dsync.synchronization.impl.zk.ZKConfiguration;
import dsync.synchronization.impl.zk.ZKDoubleBarrier;
import dsync.synchronization.CyclicBarrier;
import dsync.synchronization.DoubleBarrier;
import dsync.synchronization.Lock;
import dsync.synchronization.LockFactory;
import dsync.synchronization.ReadWriteLock;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestZKLockFactory implements LockFactory {

    private static final Logger log = Logger.getLogger(TestZKLockFactory.class);

    private static TestZKLockFactory factory;
    private ZKConfiguration config;
    private ZooKeeper zookeeper;
    private FactoryWatcher watcher;

    private TestZKLockFactory(ZKConfiguration config) throws IOException {
        this.config = config;
        init();
    }

    public static synchronized TestZKLockFactory getInstance(ZKConfiguration config)
            throws IOException {
        if (factory == null) {
            factory = new TestZKLockFactory(config);
        }

        return factory;
    }

    public static TestZKLockFactory newInstance(ZKConfiguration config) throws IOException {
        return new TestZKLockFactory(config);
    }

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

            // Session timeout or connection-loss strategy
            // 1. Keep a list of currently held locks by this vm.
            // 2. If session timeout or connection loss is detected set a flag in all currently held
            //    locks.
            // 3. In locks when unlock() gets called it will throw an exception indicating the lock
            //    has been broken. Now it's up to the application to properly handle this and carry
            //    out required roll backs as necessary.

            
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
            Date now = new Date();
            String strDate = sdfDate.format(now);

            log.info("Session timed out.." + strDate);
            System.out.println("Session timed out.." + System.currentTimeMillis());
/*            int retries = 5;

            Exception lastException = null;
            while (retries > 0) {
                try {
                    init();
                    System.out.println("Done..");

                    return;
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Error while re-establishing client..", e);
                    }

                    if (retries == 1) {
                        lastException = e;
                    }
                    retries--;
                }
            }

            log.error("Error while re-establishing client after "+ retries +" retries. " +
                      "Last exception : ", lastException);
            System.out.println("Error while re-establishing client after "+ retries +" retries. " +
                      "Last exception : " + lastException);*/
        }
    }

}
