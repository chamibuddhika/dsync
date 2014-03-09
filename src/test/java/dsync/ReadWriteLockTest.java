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
package dsync;

import dsync.synchronization.Lock;
import dsync.synchronization.LockException;
import dsync.synchronization.LockFactory;
import dsync.synchronization.ReadWriteLock;
import dsync.synchronization.impl.zk.ZKConfiguration;
import junit.framework.TestCase;

import java.io.IOException;

public class ReadWriteLockTest extends TestCase {

    public void testMultipleReadLocks() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        ReadWriteLock lock = fac.getReentrantReadWriteLock("/testrw");
        Lock readLock = lock.readLock();
        long firstAcquisitionTime = 0L;
        try {
            readLock.lock();
            firstAcquisitionTime = System.nanoTime();
            //System.out.println("First locked :" + firstAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        long secondAcquisitionTime = 0L;
        try {
            readLock.lock();
            secondAcquisitionTime = System.nanoTime();
            //System.out.println("Second locked :" + secondAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        readLock.unlock();
        long unlockTime = System.nanoTime();

        assertTrue("Second lock acquisition should happen after first..",
                   secondAcquisitionTime - firstAcquisitionTime > 0);
        assertTrue("Unlock should happen after second lock acquisition..",
                   unlockTime - secondAcquisitionTime > 0);

        System.out.println("Lock : Same      | Thread : Same      | Client : Same      --> Passed..\n");

    }

    public void testMultipleWriteLocks() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        ReadWriteLock lock = fac.getReentrantReadWriteLock("/testrw");
        Lock writeLock = lock.writeLock();
        long firstAcquisitionTime = 0L;
        try {
            writeLock.lock();
            firstAcquisitionTime = System.nanoTime();
            //System.out.println("First locked :" + firstAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        long secondAcquisitionTime = 0L;
        try {
            writeLock.lock();
            secondAcquisitionTime = System.nanoTime();
            //System.out.println("Second locked :" + secondAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        writeLock.unlock();
        long unlockTime = System.nanoTime();

        assertTrue("Second lock acquisition should happen after first..",
                   secondAcquisitionTime - firstAcquisitionTime > 0);
        assertTrue("Unlock should happen after second lock acquisition..",
                   unlockTime - secondAcquisitionTime > 0);

        System.out.println("Lock : Same      | Thread : Same      | Client : Same      --> Passed..\n");
    }

    public void testReadFirstMixedLocks() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        ReadWriteLock lock = fac.getReentrantReadWriteLock("/testrw");
        Lock readLock = lock.readLock();
        long firstThreadAcquisitionTime = 0L;
        try {
            readLock.lock();
            firstThreadAcquisitionTime = System.nanoTime();
            //System.out.println("First locked :" + firstAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        Lock writeLock = lock.writeLock();
        LockRunnable r = new LockRunnable(writeLock);
        Thread t = new Thread(r);
        t.start();

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        readLock.unlock();
        long firstThreadUnlockTime = System.nanoTime();

        long secondThreadAcquisitionTime = r.getAcquisitionTime();
        long secondThreadUnlockTime = r.getReleaseTime();

        while (secondThreadAcquisitionTime == 0 || secondThreadUnlockTime == 0) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            secondThreadAcquisitionTime = r.getAcquisitionTime();
            secondThreadUnlockTime = r.getReleaseTime();
        }

        assertTrue("Read lock thread unlock should happen after locking..",
                   firstThreadUnlockTime - firstThreadAcquisitionTime > 0);
        assertTrue("Write lock should happen after read lock unlocking..",
                   secondThreadAcquisitionTime - firstThreadUnlockTime > 0);
        assertTrue("Write thread unlock should happen after write thread locking..",
                   secondThreadUnlockTime - secondThreadAcquisitionTime > 0);

    }

    public void testWriteFirstMixedLocks() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        ReadWriteLock lock = fac.getReentrantReadWriteLock("/testrw");
        Lock writeLock = lock.writeLock();
        long firstThreadAcquisitionTime = 0L;
        try {
            writeLock.lock();
            firstThreadAcquisitionTime = System.nanoTime();
            //System.out.println("First locked :" + firstAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        Lock readLock = lock.readLock();
        LockRunnable r = new LockRunnable(readLock);
        Thread t = new Thread(r);
        t.start();

        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        writeLock.unlock();
        long firstThreadUnlockTime = System.nanoTime();

        long secondThreadAcquisitionTime = r.getAcquisitionTime();
        long secondThreadUnlockTime = r.getReleaseTime();

        while (secondThreadAcquisitionTime == 0 || secondThreadUnlockTime == 0) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            secondThreadAcquisitionTime = r.getAcquisitionTime();
            secondThreadUnlockTime = r.getReleaseTime();
        }

        assertTrue("Write lock thread unlock should happen after locking..",
                   firstThreadUnlockTime - firstThreadAcquisitionTime > 0);
        assertTrue("Read lock should happen after write lock unlocking..",
                   secondThreadAcquisitionTime - firstThreadUnlockTime > 0);
        assertTrue("Read thread unlock should happen after read thread locking..",
                   secondThreadUnlockTime - secondThreadAcquisitionTime > 0);
    }

    private static class LockRunnable implements Runnable {

        private ZKConfiguration config;
        private Lock lock;
        private long acquisitionTime;
        private long releaseTime;
        private boolean differentClient;

        public LockRunnable(ZKConfiguration config, boolean differentClient) {
            this.config = config;
            this.differentClient = differentClient;
        }

        public LockRunnable(Lock lock) {
            this.lock = lock;
        }

        public void run() {
            LockFactory fac = null;
            if (config != null) {
                if (differentClient) {
                    try {
                        fac = TestZKLockFactory.newInstance(config);
                    } catch (IOException e) {
                        return;
                    }
                } else {
                    try {
                        fac = TestZKLockFactory.getInstance(config);
                    } catch (IOException e) {
                        return;
                    }
                }
            }

            if (fac != null) {
                lock = fac.getReentrantLock("/testrw");
            }

            try {
                lock.lock();
                acquisitionTime = System.nanoTime();
            } catch (LockException e) {
                e.printStackTrace();
            }


            try {
                lock.unlock();
            } catch (LockException e) {
                e.printStackTrace();
            }
            releaseTime = System.nanoTime();

        }

        public long getAcquisitionTime() {
            return this.acquisitionTime;
        }

        public long getReleaseTime() {
            return this.releaseTime;
        }
    }

}
