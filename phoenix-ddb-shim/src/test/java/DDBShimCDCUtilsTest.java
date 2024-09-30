import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.DAY_MS;
import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.getTimeWindow;

public class DDBShimCDCUtilsTest {

    /**
     * Test that time windows [start, end] are generated correctly for given timestamp t.
     * start <= t <= end and window length is 24h in ms.
     */
    @Test(timeout = 120000)
    public void testGetTimeWindow() {
        testGetTimeWindowHelper(0, true);
        testGetTimeWindowHelper(1, false);
        testGetTimeWindowHelper(1, true);
        testGetTimeWindowHelper(2, false);
        testGetTimeWindowHelper(2, true);
        testGetTimeWindowHelper(5, false);
        testGetTimeWindowHelper(5, true);
        testGetTimeWindowHelper(10, false);
        testGetTimeWindowHelper(10, true);
        testGetTimeWindowHelper(20, false);
        testGetTimeWindowHelper(20, true);
        testGetTimeWindowHelper(30, false);
        testGetTimeWindowHelper(30, true);
    }

    private void testGetTimeWindowHelper(int lookbackDays, boolean useJitter) {
        long currentTime = System.currentTimeMillis();
        long cdcEnabledTime = currentTime - lookbackDays*DAY_MS;
        if (useJitter) cdcEnabledTime -= ThreadLocalRandom.current().nextInt(1000000);
        long[] window = getTimeWindow(cdcEnabledTime, currentTime);
        Assert.assertTrue(currentTime >= window[0] && currentTime <= window[1]);
        Assert.assertEquals(DAY_MS, window[1]-window[0]+1);
    }
}
