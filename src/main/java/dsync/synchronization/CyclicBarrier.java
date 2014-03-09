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
package dsync.synchronization;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

public interface CyclicBarrier {

    public int await() throws BrokenBarrierException;

    public int await(long timeout, TimeUnit unit);

    public int getNumberWaiting();

    public int getParties();

    public boolean isBroken();

    public void setBroken(boolean isBroken);

    public void reset();

}
