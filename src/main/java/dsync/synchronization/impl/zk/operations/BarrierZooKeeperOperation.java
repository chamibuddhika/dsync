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

public class BarrierZooKeeperOperation implements ZooKeeperOperation {

    private static final Logger LOG = Logger.getLogger(BarrierZooKeeperOperation.class);

    private static final int WAIT_TIME = 1000;

    private OperationConfig config;
    private Runnable barrierAction;
    int parties;

    public BarrierZooKeeperOperation(OperationConfig config, int parties, Runnable barrierAction) {

        this.config = config;
        this.parties = parties;
        this.barrierAction = barrierAction;

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
        String nodePrefix = "cb--" + ip + "-" + vmProcess + "-" + Thread.currentThread().getId() +
                            "--";
        //threadLocalId.set(nodePrefix);
        //System.out.println("Set node id : " + nodeId.get());

        String myNodeName = getOwnedNodeName(nodePrefix);

        if (myNodeName == null) {
            config.getZookeeper().create(config.getDir() + "/" + nodePrefix, config.getTestData(),
                                         config.getAcls(), EPHEMERAL_SEQUENTIAL);
            System.out.println("Created node with prefix : " + nodePrefix);
        }


        myNodeName = getOwnedNodeName(nodePrefix);
        int arrivalIndex = getNumberOfChildNodes();
/*        List<String> names = config.getZookeeper().getChildren(config.getDir(), false);

        if (names != null) {
            arrivalIndex = names.size();
        }

        String myNodeName = null;
        for (String name : names) {
            if (name.startsWith(nodePrefix)) {
                myNodeName = name;
                break;
            }
        }*/

        BarrierWatcher watcher = new BarrierWatcher(myNodeName);
        config.getZookeeper().exists(config.getDir() + "/ready", watcher); 

        if (arrivalIndex < parties) {
            synchronized (myNodeName.intern()) {
                try {
                    while (!watcher.isBarrierComplete()) {
                        myNodeName.intern().wait(WAIT_TIME);
                        //System.out.println("Waiting at " + myNodeName);
                    }
                } catch (InterruptedException e) {
                    // Create special broken flag to indicate that the barrier has been broken
                    // Wake up waiting threads by creating ready node
                    throw e;
                }
            }
        } else {
            config.getZookeeper().create(config.getDir() + "/ready", config.getTestData(),
                                         config.getAcls(), EPHEMERAL);
            System.out.println("Created ready node at index:" + arrivalIndex);

            if (barrierAction != null) { // Run this in the same thread to ensure barrier action is completed
                barrierAction.run();     //  before next use of barrier
            }
        }

        return Boolean.TRUE;
        //return (parties - arrivalIndex);  //To change body of implemented methods use File | Settings | File Templates.

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

    private class BarrierWatcher implements Watcher {

        private final String localLock;

        private boolean barrierComplete;

        BarrierWatcher(String localLock) {
            this.localLock = localLock;
        }

        public void process(WatchedEvent event) {
            barrierComplete = Boolean.TRUE;

            synchronized (localLock.intern()) {

                try {
                    Stat stat = config.getZookeeper().exists(config.getDir() + "/ready", false);
                    if (stat != null) {
                        config.getZookeeper().delete(config.getDir() + "/ready", 0);
                        System.out.println("Deleted ready node..");
                    }
                } catch (Exception ignored) {
                    //ignored.printStackTrace();
                    // Ignored
                }

                try {
                    Stat stat = config.getZookeeper().exists(config.getDir() + "/" + localLock, false);
                    if (stat != null) {
                        config.getZookeeper().delete(config.getDir() + "/" + localLock, 0);
                        System.out.println("Delete node with id " + localLock);
                    }
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
                // Delete ready node if existing
                // Delete process node if existing
                // Check whether wake up is due to a broken barrier
/*                List<String> names = null;
                do  {
                    try {
                        names = zookeeper.getChildren(dir, false);
                    } catch (KeeperException e) {  // Ignored
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    } catch (InterruptedException e) { // Ignored
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                } while (names != null && names.size() > 0) ;*/

                localLock.intern().notifyAll();
            }
        }

        public boolean isBarrierComplete() {
            return this.barrierComplete;
        }
    }

}
