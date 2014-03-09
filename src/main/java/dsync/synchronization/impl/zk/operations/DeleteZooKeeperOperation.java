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

public class DeleteZooKeeperOperation implements ZooKeeperOperation {

    private static final Logger log = Logger.getLogger(LockZooKeeperOperation.class);

    private OperationConfig config;
    private ThreadLocal<String> perThreadNodeId;

    public DeleteZooKeeperOperation(OperationConfig config) {
        this.config = config;
        this.perThreadNodeId = config.getThreadLocalId();
    }

    public boolean execute() throws KeeperException, InterruptedException {
        config.getZookeeper().delete(perThreadNodeId.get(), -1);
        //System.out.println("Deleted id: " + id.get());
        return Boolean.TRUE;
    }
}
