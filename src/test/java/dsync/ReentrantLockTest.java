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

import dsync.synchronization.impl.zk.ZKConfiguration;
import dsync.synchronization.Lock;
import dsync.synchronization.LockException;
import dsync.synchronization.LockFactory;
import junit.framework.TestCase;

import java.io.IOException;

public class ReentrantLockTest extends TestCase {

    public void testSameLockThreadClientLocking() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        Lock lock = fac.getReentrantLock("/test");
        long firstAcquisitionTime = 0L;
        try {
            lock.lock();
            firstAcquisitionTime = System.nanoTime();
            //System.out.println("First locked :" + firstAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        long secondAcquisitionTime = 0L;
        try {
            lock.lock();
            secondAcquisitionTime = System.nanoTime();
            //System.out.println("Second locked :" + secondAcquisitionTime);
        } catch (LockException e) {
            e.printStackTrace();
        }

        lock.unlock();
        long unlockTime = System.nanoTime();

        assertTrue("Second lock acquisition should happen after first..",
                   (secondAcquisitionTime - firstAcquisitionTime > 0));
        assertTrue("Unlock should happen after second lock acquisition..",
                   (unlockTime - secondAcquisitionTime > 0));

        System.out.println("Lock : Same      | Thread : Same      | Client : Same      --> Passed..\n");
        //System.out.println("Both unlocked :" + System.currentTimeMillis());

    }

    public void testSameLockDifferentThreadSameClientLocking() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        Lock lock = fac.getReentrantLock("/test");
        long firstThreadAcquisitionTime = 0L;
        try {
            lock.lock();
            firstThreadAcquisitionTime = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
        }

        LockRunnable r = new LockRunnable(lock);
        Thread t = new Thread(r);
        t.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        lock.unlock();

        long firstThreadUnlockTime = System.nanoTime();

        long secondThreadAcquisitionTime = r.getAcquisitionTime();
        long secondThreadUnlockTime = r.getReleaseTime();

        while (secondThreadAcquisitionTime == 0 && secondThreadUnlockTime == 0) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            secondThreadAcquisitionTime = r.getAcquisitionTime();
            secondThreadUnlockTime = r.getReleaseTime();
        }

        assertTrue("First thread unlock should happen after locking..",
                   (firstThreadUnlockTime - firstThreadAcquisitionTime) > 0);
        assertTrue("Second thread lock should happen after first thread unlocking..",
                   (secondThreadAcquisitionTime - firstThreadUnlockTime) > 0);
        assertTrue("Second thread unlock should happen after second thread locking..",
                   (secondThreadUnlockTime - secondThreadAcquisitionTime) > 0);

        System.out.println("Lock : Same      | Thread : Different | Client : Same      --> Passed..\n");
    }

    public void testDifferentLockSameThreadClientLock() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        Lock lock = fac.getReentrantLock("/test");
        long firstAquistionTime = 0L;
        try {
            lock.lock();
            firstAquistionTime = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
        }

        lock = fac.getReentrantLock("/test"); // Obtain a new lock for this thread and try locking again
        long secondAquisitionTime = 0L;
        try {
            lock.lock();
            secondAquisitionTime = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
            fail("Failed at second lock acquisition..");
        }

        lock.unlock();
        long unlockTime = System.nanoTime();

        assertTrue("Second lock acquisition should happen after first..",
                   (secondAquisitionTime - firstAquistionTime > 0));
        assertTrue("Unlock should happen after second lock acquisition..",
                   (unlockTime - secondAquisitionTime > 0));

        System.out.println("Lock : Different | Thread : Same      | Client : Same      --> Passed..\n");

    }

    public void testDifferentLockThreadSameClientLocking() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        Lock lock = fac.getReentrantLock("/test");
        long firstThreadAcquisitionTime = 0L;
        try {
            lock.lock();
            firstThreadAcquisitionTime = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
        }

        LockRunnable r = new LockRunnable(config, false);
        Thread t = new Thread(r);
        t.start();

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        lock.unlock();
        long firstThreadUnlockTime = System.nanoTime();
        long secondThreadAcquisitionTime = r.getAcquisitionTime();
        long secondThreadUnlockTime = r.getReleaseTime();

        while (secondThreadAcquisitionTime == 0 && secondThreadUnlockTime == 0) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            secondThreadAcquisitionTime = r.getAcquisitionTime();
            secondThreadUnlockTime = r.getReleaseTime();
        }

        assertTrue("First thread unlock should happen after locking..",
                   (firstThreadUnlockTime - firstThreadAcquisitionTime) > 0);
        assertTrue("Second thread lock should happen after first thread unlocking..",
                   (secondThreadAcquisitionTime - firstThreadUnlockTime) > 0);
        assertTrue("Second thread unlock should happen after second thread locking..",
                   (secondThreadUnlockTime - secondThreadAcquisitionTime) > 0);

        System.out.println("Lock : Different | Thread : Different | Client : Same      --> Passed..\n");
    }

    public void testDifferentLockThreadClientLocking() throws IOException, LockException {
        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        Lock lock = fac.getReentrantLock("/test");
        long firstThreadAcquisitionTime = 0L;
        try {
            lock.lock();
            firstThreadAcquisitionTime = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
        }

        LockRunnable r = new LockRunnable(config, true);
        Thread t = new Thread(r);
        t.start();

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        lock.unlock();
        long firstThreadUnlockTime = System.nanoTime();
        long secondThreadAcquisitionTime = r.getAcquisitionTime();
        long secondThreadUnlockTime = r.getReleaseTime();

        while (secondThreadAcquisitionTime == 0 && secondThreadUnlockTime == 0) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            secondThreadAcquisitionTime = r.getAcquisitionTime();
            secondThreadUnlockTime = r.getReleaseTime();
        }

        assertTrue("First thread unlock should happen after locking..",
                   (firstThreadUnlockTime - firstThreadAcquisitionTime) > 0);
        assertTrue("Second thread lock should happen after first thread unlocking..",
                   (secondThreadAcquisitionTime - firstThreadUnlockTime) > 0);
        assertTrue("Second thread unlock should happen after second thread locking..",
                   (secondThreadUnlockTime - secondThreadAcquisitionTime) > 0);

        System.out.println("Lock : Different | Thread : Different | Client : Different --> Passed..\n");
    }

    public void testDifferentLockThreadClientMultipleLocks() throws IOException {

        int concurrency = 10;

        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);

        LockRunnable[] runnables = new LockRunnable[concurrency - 1];

        for (int i = 0; i < (concurrency - 1); i++) {
            runnables[i] = new LockRunnable(config, true);
        }

        long[] times = new long[concurrency];

        LockFactory fac = TestZKLockFactory.getInstance(config);
        Lock lock = fac.getReentrantLock("/test");
        try {
            lock.lock();
            times[0] = System.nanoTime();
        } catch (LockException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Runnable r : runnables) {
            Thread t = new Thread(r);
            t.start();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            lock.unlock();
        } catch (LockException e) {
            e.printStackTrace();
        }

        LockRunnable last = runnables[concurrency - 2];

        System.out.println("Last acquisition time : " + last.getAcquisitionTime());
        while (last.getAcquisitionTime() == 0L) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //System.out.println("LAQ : " + last.getAcquisitionTime());
        }

        for (int i = 1; i < concurrency; i++) {
            times[i] = runnables[i - 1].getAcquisitionTime();

            System.out.println("Acqusition time of thread " + i + " : " + runnables[i - 1].getAcquisitionTime());
        }

        for (int i = 1; i < concurrency; i++) {
            if (times[i] < times[i - 1]) {
                assertTrue("Locks are not acquired in arrival order..", false);
            }
        }
    }

    private class LockRunnable implements Runnable {

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
                lock = fac.getReentrantLock("/test");
            }

            try {
                lock.lock();
                acquisitionTime = System.nanoTime();
                System.out.println("time : " + acquisitionTime);
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
