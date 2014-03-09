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

public class BarrierExitZooKeeperOperation implements ZooKeeperOperation {

    private static final Logger log = Logger.getLogger(BarrierEntryZooKeeperOperation.class);

    private static final int WAIT_TIME = 1000;

    private OperationConfig config;
    int parties;

    public BarrierExitZooKeeperOperation(OperationConfig config, int parties) {
        this.config = config;
        this.parties = parties;
    }

    public boolean execute() throws KeeperException, InterruptedException {

        String ip;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            ip = "127.0.0.1";
        }

        String vmProcess = ManagementFactory.getRuntimeMXBean().getName();
        String nodePrefix = "db--" + ip + "-" + vmProcess + "-" + Thread.currentThread().getId() +
                            "--";

        while (true) {
            List<String> names = config.getZookeeper().getChildren(config.getDir(), false);

            if (names == null || (names != null && names.size() == 0)) {

                if (log.isDebugEnabled()) {
                    log.debug("Node with prefix " + nodePrefix + " exited");
                }

                System.out.println("Node with prefix " + nodePrefix + " exited");

                return Boolean.TRUE;
            } else if (names.size() == 1) {
                System.out.println("Node prefix : " + nodePrefix);
                if (names.get(0).startsWith(nodePrefix)) { // Only current node is present. Delete it and exit.
                    config.getZookeeper().delete(config.getDir() + "/" + names.get(0), 0);

                    if (log.isDebugEnabled()) {
                        log.debug("Last node with prefix " + nodePrefix + " exited..");
                    }

                    System.out.println("Last node with prefix " + nodePrefix + " exited..");

                    return Boolean.TRUE;
                }
            }

            String myNode = getOwnedNodeName(nodePrefix);

            String minNode = getLowestSequenceNode(names);

            if (log.isTraceEnabled()) {
                log.trace("Current node : " + myNode + "\n Min node : " + minNode + " \n");

                for (int i = 0; i < names.size(); i++) {
                    log.trace("Node " + i + " = " + names.get(i) + "\n");
                }
            }

            System.out.println("Current node : " + myNode + " Min node : " + minNode );
/*            for (int i = 0; i < names.size(); i++) {
                System.out.println("Node " + i + " = " + names.get(i));
            }*/


            // 'myNode' can be null for higher sequenced nodes where node is deleted before wait
            if (myNode != null) {
                synchronized (myNode) {
                    if (myNode.equals(minNode)) {
                        String maxNode = getHighestSequenceNode(names);

                        System.out.println("Max node : " + maxNode);

                        Stat stat = config.getZookeeper().exists(config.getDir() + "/" + maxNode,
                                                                 new BarrierExitWatcher(myNode));

                        if (stat != null) {
                            // Wait time is to allow the thread to awake again and finish if the max
                            // node got deleted while checking for the existence of max node because
                            // that operation is not atomic with wait operation.
                            myNode.wait(WAIT_TIME);
                        }
                    } else {
                        config.getZookeeper().delete(config.getDir() + "/" + myNode, 0);
                        System.out.println("Deleted my node : " + myNode);
                        Stat stat = config.getZookeeper().exists(config.getDir() + "/" + minNode,
                                                                 new BarrierExitWatcher(myNode));

                        if (stat != null) {
                            // Wait time is to allow the thread to awake again and finish if the min
                            // node got deleted while checking for the existence of min node because
                            //  that operation is not atomic with wait operation.
                            myNode.wait(WAIT_TIME);
                        }
                    }
                }
            }
        }
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

    private String getHighestSequenceNode(List<String> names) {
        String maxNode = null;
        long maxSeqeunceId = 0;

        for (String name : names) {
            String[] tokens = name.split("--");
            if (tokens != null && tokens.length == 3) {
                String sequenceIdStr = tokens[2];
                long sequenceId = Long.parseLong(sequenceIdStr);

                if (sequenceId > maxSeqeunceId) {
                    maxSeqeunceId = sequenceId;
                    maxNode = name;
                }
            }
        }

        return maxNode;
    }

    private String getLowestSequenceNode(List<String> names) {
        String minNode = null;
        long minSeqeunceId = Long.MAX_VALUE;

        for (String name : names) {
            String[] tokens = name.split("--");
            if (tokens != null && tokens.length == 3) {
                String sequenceIdStr = tokens[2];
                long sequenceId = Long.parseLong(sequenceIdStr);

                if (sequenceId < minSeqeunceId) {
                    minSeqeunceId = sequenceId;
                    minNode = name;
                }
            }
        }

        return minNode;
    }

    private class BarrierExitWatcher implements Watcher {

        private final String localLock;

        BarrierExitWatcher(String localLock) {
            this.localLock = localLock;
        }

        public void process(WatchedEvent event) {

            synchronized (localLock.intern()) {
                localLock.intern().notifyAll();
            }
        }
    }

}
