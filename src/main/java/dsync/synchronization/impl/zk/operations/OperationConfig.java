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

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public class OperationConfig {

    private ThreadLocal<String> threadLocalId;
    private List<ACL> acls;
    private byte[] testData = {0x12, 0x34};
    private ZooKeeper zookeeper;
    private String dir;

    public OperationConfig(ThreadLocal<String> ids, List<ACL> acls, ZooKeeper zookeeper, String dir) {
        this.setThreadLocalId(ids);
        this.setAcls(acls);
        this.setZookeeper(zookeeper);
        this.setDir(dir);
    }

    public ThreadLocal<String> getThreadLocalId() {
        return threadLocalId;
    }

    public void setThreadLocalId(ThreadLocal<String> threadLocalId) {
        this.threadLocalId = threadLocalId;
    }

    public List<ACL> getAcls() {
        return acls;
    }

    public void setAcls(List<ACL> acls) {
        this.acls = acls;
    }

    public byte[] getTestData() {
        return testData;
    }

    public void setTestData(byte[] testData) {
        this.testData = testData;
    }

    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }
}
