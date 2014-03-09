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
package dsync.synchronization.impl.zk.operations;

import dsync.synchronization.impl.zk.ZNodeName;
import dsync.synchronization.impl.zk.ZooKeeperOperation;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;

public class WriteLockZooKeeperOperation implements ZooKeeperOperation {

    private static final Logger log = Logger.getLogger(WriteLockZooKeeperOperation.class);

    private static final int WAIT_TIME = 1000;

    boolean isBlocking;
    OperationConfig config;
    ThreadLocal perThreadNodeId;

    public WriteLockZooKeeperOperation(boolean isBlocking, OperationConfig config) {
        this.isBlocking = isBlocking;
        this.config = config;
        this.perThreadNodeId = config.getThreadLocalId();
    }


    /* find if we have been created earler if not create our node
    *
    * @param prefix    the prefix node
    * @param zookeeper teh zookeeper client
    * @param dir       the dir paretn
    * @throws org.apache.zookeeper.KeeperException
    *
    * @throws InterruptedException
    */

    private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir)
            throws KeeperException, InterruptedException {
        List<String> names = zookeeper.getChildren(dir, false);

        // Diagnostics for children
        for (String name : names) {
            System.out.println("Prefix : " + prefix + " Child : " + name);
        }

        for (String name : names) {
            if (name.startsWith(prefix)) {
                perThreadNodeId.set(dir + "/" + name);
                //id = dir + "/" + name;
                if (log.isDebugEnabled()) {
                    log.debug("Found id created last time: " + perThreadNodeId.get());
                    //System.out.println("I hit at : " + id.get());
                }
                break;
            }
        }
        if (perThreadNodeId.get() == null) {
            String idStr = zookeeper.create(dir + "/" + prefix, config.getTestData(),
                                            config.getAcls(), PERSISTENT_SEQUENTIAL);
            perThreadNodeId.set(idStr);

            if (log.isDebugEnabled()) {
                log.debug("Created id: " + perThreadNodeId.get());
                //System.out.println("Created id:" + id.get());
            }
        }

    }

    private boolean nodeExists(String nodeId) throws InterruptedException, KeeperException {
        List<String> names = config.getZookeeper().getChildren(config.getDir(), false);

        for (String name : names) {
            if (name.startsWith(nodeId)) {
                return true;
            }
        }

        return false;
    }

    private String getNodeName(String path) {
        int idx = path.lastIndexOf("/");

        String nodeName = path;
        if (idx >= 0) {
            nodeName = path.substring(idx + 1);
        }

        return nodeName;
    }

    /* the command that is run and retried for actually
    * obtaining the lock
    *
    * @return if the command was successful or not
    */

    public boolean execute() throws KeeperException, InterruptedException {
        do {

            //long sessionId = zookeeper.getSessionId();
            String ip;
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                ip = "127.0.0.1";
            }

            String vmProcess = ManagementFactory.getRuntimeMXBean().getName();
            String prefix = "write--" + ip + "-" + vmProcess + "-" + Thread.currentThread().getId() +
                            "--";
            //System.out.println("Current thread id:" + Thread.currentThread().getId());
            // lets try look up the current ID if we failed
            // in the middle of creating the znode
            findPrefixInChildren(prefix, config.getZookeeper(), config.getDir());
            ZNodeName idName = new ZNodeName(perThreadNodeId.get().toString());

            if (perThreadNodeId.get() != null) {
                List<String> names = config.getZookeeper().getChildren(config.getDir(), false);
                if (names.isEmpty()) {
                    log.warn("No children in: " + config.getDir() + " when we've just " +
                             "created one! Lets recreate it...");
                    // lets force the recreation of the id
                    perThreadNodeId.set(null);
                } else {
                    // lets sort them explicitly (though they do seem to come back in order ususally :)
                    SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                    for (String name : names) {
                        sortedNames.add(new ZNodeName(config.getDir() + "/" + name));
                    }
                    //ownerId = sortedNames.first().getName();

                    SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                    //lessThanMe.remove(idName);

                    if (!lessThanMe.isEmpty()) {
                        ZNodeName lastChildName = lessThanMe.last();
                        String lastChildId = lastChildName.getName();
                        if (log.isDebugEnabled()) {
                            log.debug("watching less than me node: " + lastChildId);
                        }

                        synchronized (idName.getName().intern()) {
                            Stat stat = config.getZookeeper().exists(lastChildId,
                                                                     new LockWatcher(idName.getName().
                                                                             intern()));  // Insert node prefix at the constructor of LockWatcher
                            if (stat != null) {

                                // This looping is to guard against the possibility that the
                                // lastChildId node already gotten deleted before the wait,
                                // causing this thread to hang forever because there will not be
                                // any notification for it to wake up since node has already been
                                // deleted.
                                String lastNode = getNodeName(lastChildId);
                                while (nodeExists(lastNode)) {
                                    if (isBlocking) {
                                        idName.getName().intern().wait(WAIT_TIME);
                                    }
                                }

                                return Boolean.FALSE;
                            } else {
                                log.warn("Could not find the" +
                                         " stats for less than me: " + lastChildName.getName());
                                perThreadNodeId.set(null);
                            }
                        }
                    } else {
                        return Boolean.TRUE;
                    }
                }
            }
        } while (perThreadNodeId.get() == null);

        return Boolean.FALSE;
    }

    /**
     * the watcher called on
     * getting watch while watching
     * my predecessor
     */
    private class LockWatcher implements Watcher {

        private final String localLock;

        LockWatcher(String localLock) {
            this.localLock = localLock;
        }

        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            log.debug("Watcher fired on path: " + event.getPath() + " state: " +
                      event.getState() + " type " + event.getType());
            try {
                synchronized (localLock) {  // Awake blocked thread to retry aquiring the distributed lock
                    localLock.intern().notifyAll();
                }
            } catch (Exception e) {
                log.warn("Failed to acquire lock: " + e, e);
            }
        }
    }

}


