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

import org.apache.zookeeper.data.ACL;

import java.util.List;

public class ZKConfiguration {

    private List<ACL> acls;

    private String connectionString;

    private int timeout;

    public ZKConfiguration(String connectionString, int timeout, List<ACL> acls) {
        this.setConnectionString(connectionString);
        this.setTimeout(timeout);
        this.setAcls(acls);
    }

    public ZKConfiguration() {

    }

    public List<ACL> getAcls() {
        return acls;
    }

    public void setAcls(List<ACL> acls) {
        this.acls = acls;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
