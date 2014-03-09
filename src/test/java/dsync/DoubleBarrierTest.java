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

import dsync.synchronization.DoubleBarrier;
import dsync.synchronization.InitializationException;
import dsync.synchronization.LockFactory;
import dsync.synchronization.impl.zk.ZKConfiguration;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;

public class DoubleBarrierTest extends TestCase {

                
    public void testDoubleBarrier() throws IOException, InitializationException {

        int parties = 10;

        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        DoubleBarrier barrier = fac.getDoubleBarrier("/double", parties);

        long[][] stats = new long[parties][3];

        for (int i = 0; i < (parties - 1); i++) {
            BarrierConfig barrierConfig = new BarrierConfig(stats, i, parties);
            BarrierRunnable r = new BarrierRunnable(barrier, barrierConfig);
            Thread t = new Thread(r);
            t.start();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            //System.out.println("Try lock .. :" + System.currentTimeMillis());
            long beforeTime = System.nanoTime();
            barrier.enter();
            long afterTime = System.nanoTime();

            try {
                Thread.sleep(new Random().nextInt(20));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                barrier.leave();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            long endTime = System.nanoTime();

            stats[parties - 1][0] = beforeTime;
            stats[parties - 1][1] = afterTime;
            stats[parties - 1][2] = endTime;

        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        while (isArrayEmpty(stats)) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long maxBefore = 0;
        long minAfter = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxBefore < stats[i][0]) {
                maxBefore = stats[i][0];
            }

            if (minAfter > stats[i][1]) {
                minAfter = stats[i][1];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minAfter - maxBefore > 0);

        long maxAfter = 0;
        long minEnd = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxAfter < stats[i][1]) {
                maxAfter = stats[i][1];
            }

            if (minEnd > stats[i][2]) {
                minEnd = stats[i][2];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minEnd - maxAfter > 0);

    }

    public void testDoubleBarrierDifferentInstances()
            throws IOException, InitializationException {

        int parties = 10;

        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        long[][] stats = new long[parties][3];

        for (int i = 0; i < (parties - 1); i++) {
            //DoubleBarrier barrier = fac.getDoubleBarrier("/double", parties);
            BarrierConfig barrierConfig = new BarrierConfig(stats, i, parties);
            BarrierRunnable r = new BarrierRunnable(config, barrierConfig, false);
            Thread t = new Thread(r);
            t.start();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DoubleBarrier barrier = fac.getDoubleBarrier("/double", parties);

        try {
            //System.out.println("Try lock .. :" + System.currentTimeMillis());
            long beforeTime = System.nanoTime();
            barrier.enter();
            long afterTime = System.nanoTime();

            try {
                Thread.sleep(new Random().nextInt(20));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                barrier.leave();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            long endTime = System.nanoTime();

            stats[parties - 1][0] = beforeTime;
            stats[parties - 1][1] = afterTime;
            stats[parties - 1][2] = endTime;

        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        while (isArrayEmpty(stats)) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long maxBefore = 0;
        long minAfter = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxBefore < stats[i][0]) {
                maxBefore = stats[i][0];
            }

            if (minAfter > stats[i][1]) {
                minAfter = stats[i][1];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minAfter - maxBefore > 0);

        long maxAfter = 0;
        long minEnd = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxAfter < stats[i][1]) {
                maxAfter = stats[i][1];
            }

            if (minEnd > stats[i][2]) {
                minEnd = stats[i][2];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minEnd - maxAfter > 0);

    }

    public void testDoubleBarrierDifferentClients()
            throws IOException, InitializationException {

        int parties = 10;

        ZKConfiguration config = new ZKConfiguration("localhost:2181", 1000000, null);
        LockFactory fac = TestZKLockFactory.getInstance(config);

        long[][] stats = new long[parties][3];

        for (int i = 0; i < (parties - 1); i++) {
            BarrierConfig barrierConfig = new BarrierConfig(stats, i, parties);
            BarrierRunnable r = new BarrierRunnable(config, barrierConfig, true);
            Thread t = new Thread(r);
            t.start();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DoubleBarrier barrier = fac.getDoubleBarrier("/double", parties);

        try {
            //System.out.println("Try lock .. :" + System.currentTimeMillis());
            long beforeTime = System.nanoTime();
            barrier.enter();
            long afterTime = System.nanoTime();

            try {
                Thread.sleep(new Random().nextInt(20));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                barrier.leave();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            long endTime = System.nanoTime();

            stats[parties - 1][0] = beforeTime;
            stats[parties - 1][1] = afterTime;
            stats[parties - 1][2] = endTime;

        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        while (isArrayEmpty(stats)) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long maxBefore = 0;
        long minAfter = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxBefore < stats[i][0]) {
                maxBefore = stats[i][0];
            }

            if (minAfter > stats[i][1]) {
                minAfter = stats[i][1];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minAfter - maxBefore > 0);

        long maxAfter = 0;
        long minEnd = Long.MAX_VALUE;
        for (int i = 0; i < parties; i++) {

            if (maxAfter < stats[i][1]) {
                maxAfter = stats[i][1];
            }

            if (minEnd > stats[i][2]) {
                minEnd = stats[i][2];
            }

        }

        assertTrue("Each thread should start only after barrier completion..",
                   minEnd - maxAfter > 0);

    }

    private boolean isArrayEmpty(long[][] stats) {
        for (int i = 0; i < stats.length ; i++) {
            for (int j = 0; j < stats[i].length ; j++) {

                if (stats[i][j] == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    private class BarrierRunnable implements Runnable {

        private ZKConfiguration config;
        private DoubleBarrier barrier;
        private BarrierConfig barrierConfig;
        private boolean differentClient;

        public BarrierRunnable(ZKConfiguration config, BarrierConfig barrierConfig,
                               boolean differentClient) {
            this.config = config;
            this.barrierConfig = barrierConfig;
            this.differentClient = differentClient;
        }

        public BarrierRunnable(DoubleBarrier barrier, BarrierConfig barrierConfig) {
            this.barrier = barrier;
            this.barrierConfig = barrierConfig;
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
                try {
                    barrier = fac.getDoubleBarrier("/double", barrierConfig.getParties());
                } catch (InitializationException e) {
                    e.printStackTrace();
                    return;
                }
            }

            try {
                //System.out.println("Try lock .. :" + System.currentTimeMillis());
                long beforeTime = System.nanoTime();
                barrier.enter();
                long afterTime = System.nanoTime();

                try {
                    Thread.sleep(new Random().nextInt(20));
                } catch (InterruptedException ignored) {
                    // ignored
                }

                barrier.leave();

                long endTime = System.nanoTime();

                long[][] stats = barrierConfig.getStats();

                stats[barrierConfig.getIndex()][0] = beforeTime;
                stats[barrierConfig.getIndex()][1] = afterTime;
                stats[barrierConfig.getIndex()][2] = endTime;

            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    private class BarrierConfig {

        private int index;
        private long[][] stats;
        private int parties;

        BarrierConfig(long[][] stats, int index, int parties) {
            this.setIndex(index);
            this.setStats(stats);
            this.setParties(parties);
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public long[][] getStats() {
            return stats;
        }

        public void setStats(long[][] stats) {
            this.stats = stats;
        }

        public int getParties() {
            return parties;
        }

        public void setParties(int parties) {
            this.parties = parties;
        }
    }

}
