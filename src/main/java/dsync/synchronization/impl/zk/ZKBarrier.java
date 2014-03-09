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
import dsync.synchronization.InitializationException;
import dsync.synchronization.impl.zk.operations.BarrierZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.OperationConfig;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

public class ZKBarrier extends ProtocolSupport implements CyclicBarrier {

    private static final Logger LOG = Logger.getLogger(ZKBarrier.class);

    private final String dir;
    private int parties;
    private boolean barrierBroken;
    private Runnable barrierAction;

    public ZKBarrier(ZooKeeper zookeeper, List<ACL> acl, String dir, int parties)
            throws InitializationException {
        super(zookeeper, acl);
        this.dir = dir;
        this.parties = parties;

        initNode();

    }

    public ZKBarrier(ZooKeeper zookeeper, List<ACL> acl, String dir, int parties,
                     Runnable barrierAction) throws InitializationException {
        super(zookeeper, acl);
        this.dir = dir;
        this.parties = parties;
        this.barrierAction = barrierAction;

        initNode();

    }

    public int await() throws BrokenBarrierException {

        OperationConfig config = new OperationConfig(null, getAcl(), zookeeper, dir);
        ZooKeeperOperation zop = new BarrierZooKeeperOperation(config, parties, barrierAction);
        boolean successful = false;
        while (!successful) {
            try {
                successful = (Boolean) retryOperation(zop);
            } catch (KeeperException e) {
                throw new BrokenBarrierException(e.getLocalizedMessage());
            } catch (InterruptedException e) {
                throw new BrokenBarrierException(e.getLocalizedMessage());
            } catch (UnknownHostException e) {
                throw new BrokenBarrierException(e.getLocalizedMessage());
            }
        }

        return parties;  //ToDO: Change to arrival index
    }

    public int await(long timeout, TimeUnit unit) {
        return 0;
    }

    public int getNumberWaiting() {
        try {
            List<String> names = zookeeper.getChildren(dir, false);
            return names.size();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();  
        }
        return 0;
    }

    public int getParties() {
        return parties;
    }

    public boolean isBroken() {
        return barrierBroken;
    }

    public void setBroken(boolean isBroken) {
        this.barrierBroken = isBroken;
    }

    public void reset() {   // Think about the usage of this method
        try {
            List<String> names = zookeeper.getChildren(dir, false);
            for (String name : names) {
                try {
                    zookeeper.delete(dir + "/" + name, 0);
                } catch (InterruptedException ignored) { // Ignore individual node deletion failure.
                    // Ignore                               Continue with other nodes.
                } catch (KeeperException ignored) {
                    // Ignored
                }
            }

        } catch (InterruptedException ignored) { // Ignoring getChildren failure
            // Ignored
        } catch (KeeperException ignored) {
            // Ignored
        }
    }

    /**
     * Creates base node for this barrier. If a node already exists created by an earlier client gets
     * and sets the number of parties of to current instance of barrier. So it is possible that the
     * number of parties may not the same as the amount passed to the constructor. Use getter method
     * to get the correct number of parties for this barrier after the initialization.
     *
     * @throws dsync.synchronization.InitializationException
     */
    private void initNode() throws InitializationException {
        try {
            Stat stat = zookeeper.exists(dir, false);
            if (stat != null) {
                byte[] data = zookeeper.getData(dir, false, stat);
                this.parties = Integer.parseInt(new String(data));
            } else {
                ensureExists(dir, new String(Integer.toString(parties)).getBytes(), getAcl(),
                                 PERSISTENT );
            }

        } catch (KeeperException e) {
            throw new InitializationException("Failed to connect to zookeeper node..", e);
        } catch (InterruptedException e) {
            throw new InitializationException("Failed to connect to zookeeper node..", e);
        } catch (UnknownHostException e) {
            throw new InitializationException("Failed to connect to zookeeper node..", e);
        }
    }

}
