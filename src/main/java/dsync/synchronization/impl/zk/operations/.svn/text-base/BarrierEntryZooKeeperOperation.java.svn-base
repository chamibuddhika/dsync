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

import dsync.synchronization.impl.zk.ZooKeeperOperation;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

public class BarrierEntryZooKeeperOperation implements ZooKeeperOperation {

    private static final Logger log = Logger.getLogger(BarrierEntryZooKeeperOperation.class);

    private static final int WAIT_TIME = 1000;

    private OperationConfig config;
    int parties;

    public BarrierEntryZooKeeperOperation(OperationConfig config, int parties) {
        this.config = config;
        this.parties = parties;
    }

    public boolean execute() throws KeeperException, InterruptedException {

        String ip;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            ip = "127.0.0.1";
        }

        String vmProcess = ManagementFactory.getRuntimeMXBean().getName();
        String nodePrefix = "db--" + ip + "-" + vmProcess + "-" + Thread.currentThread().getId() +
                            "--";

        String myNodeName = getOwnedNodeName(nodePrefix);

        if (myNodeName == null) {
            config.getZookeeper().create(config.getDir() + "/" + nodePrefix, config.getTestData(),
                                         config.getAcls(), EPHEMERAL_SEQUENTIAL);
            if (log.isDebugEnabled()) {
                log.debug("Created node with prefix : " + nodePrefix);
            }

            System.out.println("Created node with prefix : " + nodePrefix);
        }

        myNodeName = getOwnedNodeName(nodePrefix);

        BarrierEntryWatcher watcher = new BarrierEntryWatcher(myNodeName);

        config.getZookeeper().exists(config.getDir() + "/ready", watcher);
        myNodeName = getOwnedNodeName(nodePrefix);
        int arrivalIndex = getNumberOfChildNodes(); // Point A

        if (arrivalIndex < parties) {
            synchronized (myNodeName.intern()) {
                try {
                    System.out.println("Entry barrier prefix :" + nodePrefix);
                    // This check guards against the barrier being completed and nodes getting
                    // deleted during time taken from the fetching of arrival index and the
                    // (arrivalIndex < parties) check causing a false wait. If this is the case
                    // watcher flag should be set to complete thus avoiding the wait.
                    while (!watcher.isBarrierComplete()) {
                        myNodeName.intern().wait(WAIT_TIME); // Point B
                    }/* else {
                            return Boolean.TRUE;
                        }*/
                } catch (InterruptedException e) {
                    // Create special broken flag to indicate that the barrier has been broken
                    // Wake up waiting threads by creating ready node
                    throw e;
                }
            }
        } else {

            // Check before creating 'ready' node because ready node may have been created and
            // subsequently deleted between the execution of Point A and Point B. This also assumes
            // that watcher was executed before WAIT_TIME finishes which should almost always be
            // the case. But even if the 'ready' node is to be created redundantly this will not
            // affect the accuracy of barrier.
            if (!watcher.isBarrierComplete()) {
                config.getZookeeper().create(config.getDir() + "/ready", config.getTestData(),
                                             config.getAcls(), EPHEMERAL);
            }

            if (log.isDebugEnabled()) {
                log.debug("Created ready node at index:" + arrivalIndex);
            }

            System.out.println("Created ready node at index:" + arrivalIndex);
        }

        return Boolean.TRUE;

    }

    private String getOwnedNodeName(String nodePrefix)
            throws InterruptedException, KeeperException {

        List<String> names = config.getZookeeper().getChildren(config.getDir(), false);

        String myNodeName = null;
        if (names != null) {
            for (String name : names) {
                if (name.startsWith(nodePrefix)) {
                    myNodeName = name;
                    break;
                }
            }
        }

        return myNodeName;
    }

    private int getNumberOfChildNodes() throws InterruptedException, KeeperException {
        List<String> names = config.getZookeeper().getChildren(config.getDir(), false);

        if (names != null) {
            return names.size();
        }

        return 0;

    }

    private class BarrierEntryWatcher implements Watcher {

        private final String localLock;

        private boolean barrierComplete;

        BarrierEntryWatcher(String localLock) {
            this.localLock = localLock;
        }

        public void process(WatchedEvent event) {

            this.barrierComplete = Boolean.TRUE;

            try {
                synchronized (localLock.intern()) {

                    try {
                        Stat stat = config.getZookeeper().exists(config.getDir() + "/ready", false);
                        if (stat != null) {
                            config.getZookeeper().delete(config.getDir() + "/ready", 0);

                            if (log.isDebugEnabled()) {
                                log.debug("Deleted ready node with node : " + localLock);
                            }

                            System.out.println("Deleted ready node with node : " + localLock);
                        }
                    } catch (Exception ignored) {
                        // Can be ignored since during the time window of checking 'ready' node existence
                        // it may have been deleted by another client which may cause and exception here.
                        // In this case it's harmless.
                    }

                    localLock.intern().notifyAll();
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        public boolean isBarrierComplete() {
            return this.barrierComplete;
        }

    }


}
