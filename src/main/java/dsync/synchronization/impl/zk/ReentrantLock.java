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

import dsync.synchronization.Lock;
import dsync.synchronization.LockException;
import dsync.synchronization.impl.zk.operations.DeleteZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.LockZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.OperationConfig;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReentrantLock extends ProtocolSupport implements Lock {

    private static final Logger LOG = Logger.getLogger(ReentrantLock.class);

    private final String dir;
    private boolean lockBroken;
    private ThreadLocal threadLocalId = new ThreadLocal();
/*    private ZNodeName idName;
    private String ownerId;
    private String lastChildId;
    private byte[] data = {0x12, 0x34};
    private LockListener callback; // TODO: Remove callback. Guess it's not required due to true blocking nature of the lock*/
    //private LockZooKeeperOperation zop;

    public ReentrantLock(ZooKeeper zooKeeper, List<ACL> acl, String dir) {
        super(zooKeeper, acl);
        this.dir = dir;
    }

    public void lock() throws LockException {
        if (isBroken()) {
            throw new LockException("Session terminated..");
        }

        try {
            ensurePathExists(dir);
        } catch (UnknownHostException e) {
            throw new LockException("Failed to acquire lock..", e);
        }

        OperationConfig config = new OperationConfig(threadLocalId, getAcl(), zookeeper, dir);
        ZooKeeperOperation zop = new LockZooKeeperOperation(true, config);
        boolean successful = false;
        while (!successful) {
            try {
                successful = (Boolean) retryOperation(zop);
            } catch (KeeperException e) {
                throw new LockException("Failed to aquire lock..", e);
            } catch (InterruptedException e) {
                throw new LockException("Failed to aquire lock..", e);
            } catch (UnknownHostException e) {
                throw new LockException("Failed to acquire lock..", e);
            }
        }

        SessionManager.getInstance().registerSynchronizationPrimitive(this, PrimitiveType.LOCK);
    }

    public boolean tryLock() throws LockException {
        if (isBroken()) {
            throw new LockException("Session terminated..");
        }

        try {
            ensurePathExists(dir);
        } catch (UnknownHostException e) {
            throw new LockException("Failed to aquire lock..", e);
        }

        OperationConfig config = new OperationConfig(threadLocalId, getAcl(), zookeeper, dir);
        ZooKeeperOperation zop = new LockZooKeeperOperation(false, config);
        try {
            Boolean successful = (Boolean) retryOperation(zop);

            if (successful) {
                SessionManager.getInstance().registerSynchronizationPrimitive(this,
                                                                              PrimitiveType.LOCK);
            }

            return successful;

        } catch (KeeperException e) {
            throw new LockException("Failed to aquire lock..", e);
        } catch (InterruptedException e) {
            throw new LockException("Failed to aquire lock..", e);
        } catch (UnknownHostException e) {
            throw new LockException("Failed to aquire lock..", e);
        }
    }

    public boolean tryLock(long time, TimeUnit unit) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void unlock() throws LockException {

        if (!isBroken() && threadLocalId.get() != null) {
            // we don't need to retry this operation in the case of failure
            // as ZK will remove ephemeral files and we don't wanna hang
            // this process when closing if we cannot reconnect to ZK
            try {

                OperationConfig config = new OperationConfig(threadLocalId, getAcl(), zookeeper,
                                                             dir);
                ZooKeeperOperation zopdel = new DeleteZooKeeperOperation(config);
                try {
                    retryOperation(zopdel);
                } catch (UnknownHostException e) {
                    LOG.warn("Caught: " + e, e);
                    throw (RuntimeException) new RuntimeException(e.getMessage()).
                            initCause(e);
                } finally {
                    SessionManager.getInstance().unRegisterSynchronizationPrimitive(
                            this, PrimitiveType.LOCK);
                }

            } catch (InterruptedException e) {
                LOG.warn("Caught: " + e, e);
                //set that we have been interrupted.
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            } catch (KeeperException e) {
                LOG.warn("Caught: " + e, e);
                throw (RuntimeException) new RuntimeException(e.getMessage()).
                        initCause(e);
            } finally {
                threadLocalId.set(null);
                SessionManager.getInstance().unRegisterSynchronizationPrimitive(
                        this, PrimitiveType.LOCK);
            }
        } else {
            SessionManager.getInstance().unRegisterSynchronizationPrimitive(this,
                                                                            PrimitiveType.LOCK);
            throw new LockException("Lock broken..");
        }
    }

    public boolean isBroken() {
        return this.lockBroken;
    }

    public void setBroken(boolean isBroken) {
        this.lockBroken = isBroken;
    }

    /**
     * a zoookeeper operation that is mainly responsible
     * for all the magic required for locking.
     */
/*    private class LockZooKeeperOperation implements ZooKeeperOperation {

        boolean isBlocking;

        LockZooKeeperOperation(boolean isBlocking) {
            this.isBlocking = isBlocking;
        }

        *//**
     * find if we have been created earler if not create our node
     *
     * @param prefix    the prefix node
     * @param zookeeper teh zookeeper client
     * @param dir       the dir paretn
     * @throws org.apache.zookeeper.KeeperException
     *
     * @throws InterruptedException
     *//*
        private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir)
                throws KeeperException, InterruptedException {
            List<String> names = zookeeper.getChildren(dir, false);

            // Diagnostics for children
            for (String name : names) {
                System.out.println("Prefix : " + prefix + " Child : " + name);
            }

            for (String name : names) {
                if (name.startsWith(prefix)) {
                    id.set(dir + "/" + name);
                    //id = dir + "/" + name;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found id created last time: " + id.get());
                        //System.out.println("I hit at : " + id.get());
                    }
                    break;
                }
            }
            if (id.get() == null) {
                String idStr = zookeeper.create(dir + "/" + prefix, data,
                                                getAcl(), PERSISTENT_SEQUENTIAL);
                id.set(idStr);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created id: " + id.get());
                    //System.out.println("Created id:" + id.get());
                }
            }

        }

        *//**
     * the command that is run and retried for actually
     * obtaining the lock
     *
     * @return if the command was successful or not
     *//*
        public boolean execute() throws KeeperException, InterruptedException,
                                        UnknownHostException {
            do {

                //long sessionId = zookeeper.getSessionId();
                String ip = InetAddress.getLocalHost().getHostAddress();
                String prefix = "x-" + ip + Thread.currentThread().getId() + "-";
                //System.out.println("Current thread id:" + Thread.currentThread().getId());
                // lets try look up the current ID if we failed
                // in the middle of creating the znode
                findPrefixInChildren(prefix, zookeeper, dir);
                ZNodeName idName = new ZNodeName(id.get().toString());

                if (id.get() != null) {
                    List<String> names = zookeeper.getChildren(dir, false);
                    if (names.isEmpty()) {
                        LOG.warn("No children in: " + dir + " when we've just " +
                                 "created one! Lets recreate it...");
                        // lets force the recreation of the id
                        id.set(null);
                    } else {
                        // lets sort them explicitly (though they do seem to come back in order ususally :)
                        SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                        for (String name : names) {
                            sortedNames.add(new ZNodeName(dir + "/" + name));
                        }
                        ownerId = sortedNames.first().getName();

                        SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                        //lessThanMe.remove(idName);

                        if (!lessThanMe.isEmpty()) {
                            ZNodeName lastChildName = lessThanMe.last();
                            String lastChildId = lastChildName.getName();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("watching less than me node: " + lastChildId);
                            }

                            synchronized (idName.getName().intern()) {
                                Stat stat = zookeeper.exists(lastChildId, new LockWatcher(idName.getName().intern()));  // Insert node prefix at the constructor of LockWatcher
                                if (stat != null) {

                                    if (isBlocking) {
                                        idName.getName().intern().wait();
                                    }

                                    return Boolean.FALSE;
                                } else {
                                    LOG.warn("Could not find the" +
                                             " stats for less than me: " + lastChildName.getName());
                                    id.set(null);
                                }
                            }
*//*                            Stat stat = zookeeper.exists(lastChildId, new LockWatcher(idName.getName().intern()));  // Insert node prefix at the constructor of LockWatcher
                            if (stat != null) {
                                return Boolean.FALSE;
                            } else {

                            }

                            // add synchronization on node prefix (sessionID + threadID)*//*
                        } else {
                            if (isOwner()) {
                                if (callback != null) {
                                    callback.lockAcquired();
                                }

                                return Boolean.TRUE;
                            }
                        }
                    }
                }
            } while (id.get() == null);

            return Boolean.FALSE;
        }
    }

    *//**
     * the watcher called on
     * getting watch while watching
     * my predecessor
     *//*
    private class LockWatcher implements Watcher {

        private final String localLock;

        LockWatcher(String localLock) {
            this.localLock = localLock;
        }

        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            LOG.debug("Watcher fired on path: " + event.getPath() + " state: " +
                      event.getState() + " type " + event.getType());
            
            synchronized (localLock) {  // Awake blocked thread to retry aquiring the distributed lock
                localLock.intern().notifyAll();
            }
        }
    }

    *//**
     * Returns true if this node is the owner of the
     * lock (or the leader)
     *//*
    public boolean isOwner() {
        return id.get() != null && ownerId != null && id.get().toString().equals(ownerId);
    }*/

}
