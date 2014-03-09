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
import dsync.synchronization.ReadWriteLock;
import dsync.synchronization.impl.zk.operations.DeleteZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.OperationConfig;
import dsync.synchronization.impl.zk.operations.ReadLockZooKeeperOperation;
import dsync.synchronization.impl.zk.operations.WriteLockZooKeeperOperation;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReentrantReadWriteLock extends ProtocolSupport implements ReadWriteLock {

    private static final Logger LOG = Logger.getLogger(ReentrantReadWriteLock.class);

    private final String dir;
    //private ReadLockZooKeeperOperation readOp;
    private ThreadLocal readLockNodeId = new ThreadLocal();
    private ThreadLocal writeLockNodeId = new ThreadLocal();

    public ReentrantReadWriteLock(ZooKeeper zooKeeper, List<ACL> acl, String dir) {
        super(zooKeeper, acl);
        this.dir = dir;
    }

    public Lock readLock() {
        return new ReadLock(zookeeper, getAcl());
    }

    public Lock writeLock() {
        return new WriteLock(zookeeper, getAcl());
    }

    private class ReadLock extends ProtocolSupport implements Lock {

        private boolean lockBroken;

        public ReadLock(ZooKeeper zookeeper, List<ACL> acl) {
            super(zookeeper, acl);
        }

        public void lock() throws LockException {
            if (isBroken()) {
                throw new LockException("Session terminated..");
            }

            try {
                ensurePathExists(dir);
            } catch (UnknownHostException e) {
                throw new LockException("Failed to aquire lock..", e);
            }

            OperationConfig config = new OperationConfig(readLockNodeId, getAcl(), zookeeper, dir);
            ReadLockZooKeeperOperation readOp = new ReadLockZooKeeperOperation(true, config);
            boolean successful = false;
            while (!successful) {
                try {
                    successful = (Boolean) retryOperation(readOp);
                } catch (KeeperException e) {
                    throw new LockException("Failed to aquire lock..", e);
                } catch (InterruptedException e) {
                    throw new LockException("Failed to aquire lock..", e);
                } catch (UnknownHostException e) {
                    throw new LockException("Failed to aquire lock..", e);
                }
            }
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

            OperationConfig config = new OperationConfig(readLockNodeId, getAcl(), zookeeper, dir);
            ReadLockZooKeeperOperation readOp = new ReadLockZooKeeperOperation(false, config);
            try {
                return (Boolean) retryOperation(readOp);
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
            if (!isBroken() && readLockNodeId.get() != null) {
                // we don't need to retry this operation in the case of failure
                // as ZK will remove ephemeral files and we don't wanna hang
                // this process when closing if we cannot reconnect to ZK
                try {

                    OperationConfig config = new OperationConfig(readLockNodeId, getAcl(), zookeeper,
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
                }
                finally {
                    readLockNodeId.set(null);
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

    }

    private class WriteLock extends ProtocolSupport implements Lock {

        private boolean lockBroken;

        public WriteLock(ZooKeeper zookeeper, List<ACL> acl) {
            super(zookeeper, acl);
        }

        public void lock() throws LockException {
            if (isBroken()) {
                throw new LockException("Session terminated..");
            }

            try {
                ensurePathExists(dir);
            } catch (UnknownHostException e) {
                throw new LockException("Failed to aquire lock..", e);
            }

            OperationConfig config = new OperationConfig(writeLockNodeId, getAcl(), zookeeper, dir);
            WriteLockZooKeeperOperation readOp = new WriteLockZooKeeperOperation(true, config);
            boolean successful = false;
            while (!successful) {
                try {
                    successful = (Boolean) retryOperation(readOp);
                } catch (KeeperException e) {
                    throw new LockException("Failed to aquire lock..", e);
                } catch (InterruptedException e) {
                    throw new LockException("Failed to aquire lock..", e);
                } catch (UnknownHostException e) {
                    throw new LockException("Failed to aquire lock..", e);
                }
            }
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

            OperationConfig config = new OperationConfig(writeLockNodeId, getAcl(), zookeeper, dir);
            WriteLockZooKeeperOperation readOp = new WriteLockZooKeeperOperation(false, config);
            try {
                return (Boolean) retryOperation(readOp);
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
            if (!isBroken() && writeLockNodeId.get() != null) {
                // we don't need to retry this operation in the case of failure
                // as ZK will remove ephemeral files and we don't wanna hang
                // this process when closing if we cannot reconnect to ZK
                try {

                    OperationConfig config = new OperationConfig(writeLockNodeId, getAcl(), zookeeper,
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
                    writeLockNodeId.set(null);
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

    }

}

