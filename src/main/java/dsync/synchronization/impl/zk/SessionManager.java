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
import dsync.synchronization.DoubleBarrier;
import dsync.synchronization.Lock;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.List;

public class SessionManager implements Watcher {

    private static SessionManager manager;

    private final List<Lock> heldLocks;

    private final List<CyclicBarrier> heldCyclicBarriers;

    private final List<DoubleBarrier> heldDoubleBarriers;

    private SessionManager() {

        this.heldLocks = new ArrayList<Lock>();
        this.heldCyclicBarriers = new ArrayList<CyclicBarrier>();
        this.heldDoubleBarriers = new ArrayList<DoubleBarrier>();

    }

    public static synchronized SessionManager getInstance() {
        if (manager == null) {
            manager = new SessionManager();
        }

        return manager;
    }

    public void registerSynchronizationPrimitive(Object primitive, PrimitiveType type) {

        switch (type) {
            case LOCK:
                heldLocks.add((Lock) primitive);
                break;

            case CBARRIER:
                heldCyclicBarriers.add((CyclicBarrier) primitive);
                break;

            case DBARRIER:
                heldDoubleBarriers.add((DoubleBarrier) primitive);
                break;
        }
    }

    public void unRegisterSynchronizationPrimitive(Object primitive, PrimitiveType type) {

        switch (type) {
            case LOCK:
                heldLocks.remove(primitive);
                break;

            case CBARRIER:
                heldCyclicBarriers.remove(primitive);
                break;

            case DBARRIER:
                heldDoubleBarriers.remove(primitive);
                break;
        }
    }

    private void revocate() {

        for (Lock lock : heldLocks) {
            lock.setBroken(Boolean.TRUE);
        }

        for (CyclicBarrier barrier : heldCyclicBarriers) {
            barrier.setBroken(Boolean.TRUE);
        }

        for (DoubleBarrier barrier : heldDoubleBarriers) {
            barrier.setBroken(Boolean.TRUE);
        }
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.None) {
            switch (watchedEvent.getState()) {
                case Disconnected:
                case Expired:
                    revocate();
            }
        }

    }
}
