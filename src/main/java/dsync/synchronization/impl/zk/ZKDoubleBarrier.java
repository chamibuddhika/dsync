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

import dsync.synchronization.DoubleBarrier;
import dsync.synchronization.InitializationException;
import dsync.synchronization.impl.zk.operations.BarrierEntryZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.BarrierExitZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.OperationConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

public class ZKDoubleBarrier extends ProtocolSupport implements DoubleBarrier {

    private final String dir;
    private int parties;
    private boolean barrierBroken;

    public ZKDoubleBarrier(ZooKeeper zookeeper, List<ACL> acl, String dir, int parties)
            throws InitializationException {
        super(zookeeper, acl);
        this.dir = dir;
        this.parties = parties;

        initNode();

    }

    public int enter() throws BrokenBarrierException {
        OperationConfig config = new OperationConfig(null, getAcl(), zookeeper, dir);
        ZooKeeperOperation zop = new BarrierEntryZooKeeperOperation(config, parties);
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

    public void leave() throws BrokenBarrierException {
        OperationConfig config = new OperationConfig(null, getAcl(), zookeeper, dir);
        ZooKeeperOperation zop = new BarrierExitZooKeeperOperation(config, parties);
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
    }

    public boolean isBroken() {
        return barrierBroken;
    }

    public void setBroken(boolean isBroken) {
        this.barrierBroken = isBroken;
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
