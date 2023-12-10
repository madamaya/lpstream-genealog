package io.palyvos.provenance.l3stream.util.object;

public class TimestampsForLatency {
    public long ts1 = -1;
    public long ts2 = -1;
    public long ts3 = -1;
    public long ts4 = -1;
    public long ts5 = -1;
    public long ts6 = -1;
    public long ts7 = -1;
    public long ts8 = -1;
    public long ts9 = -1;
    public long ts10 = -1;
    private int count = 0;

    public TimestampsForLatency() {
    }

    public TimestampsForLatency(TimestampsForLatency tfl) {
        this.ts1 = tfl.ts1;
        this.ts2 = tfl.ts2;
        this.ts3 = tfl.ts3;
        this.ts4 = tfl.ts4;
        this.ts5 = tfl.ts5;
        this.ts6 = tfl.ts6;
        this.ts7 = tfl.ts7;
        this.ts8 = tfl.ts8;
        this.ts9 = tfl.ts9;
        this.ts10 = tfl.ts10;
        this.count = tfl.count;
    }


    public TimestampsForLatency(long ts1) {
        this.ts1 = ts1;
        this.count = 1;
    }

    public TimestampsForLatency(long ts1, long ts2) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.count = 2;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.count = 3;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.count = 4;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.count = 5;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.count = 6;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6, long ts7) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.ts7 = ts7;
        this.count = 7;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6, long ts7, long ts8) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.ts7 = ts7;
        this.ts8 = ts8;
        this.count = 8;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6, long ts7, long ts8, long ts9) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.ts7 = ts7;
        this.ts8 = ts8;
        this.ts9 = ts9;
        this.count = 9;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6, long ts7, long ts8, long ts9, long ts10) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.ts7 = ts7;
        this.ts8 = ts8;
        this.ts9 = ts9;
        this.ts10 = ts10;
        this.count = 10;
    }

    public TimestampsForLatency(long ts1, long ts2, long ts3, long ts4, long ts5, long ts6, long ts7, long ts8, long ts9, long ts10, int count) {
        this.ts1 = ts1;
        this.ts2 = ts2;
        this.ts3 = ts3;
        this.ts4 = ts4;
        this.ts5 = ts5;
        this.ts6 = ts6;
        this.ts7 = ts7;
        this.ts8 = ts8;
        this.ts9 = ts9;
        this.ts10 = ts10;
        this.count = count;
    }


    public long getTs1() {
        return ts1;
    }

    public void setTs1(long ts1) {
        this.ts1 = ts1;
    }

    public long getTs2() {
        return ts2;
    }

    public void setTs2(long ts2) {
        this.ts2 = ts2;
    }

    public long getTs3() {
        return ts3;
    }

    public void setTs3(long ts3) {
        this.ts3 = ts3;
    }

    public long getTs4() {
        return ts4;
    }

    public void setTs4(long ts4) {
        this.ts4 = ts4;
    }

    public long getTs5() {
        return ts5;
    }

    public void setTs5(long ts5) {
        this.ts5 = ts5;
    }

    public long getTs6() {
        return ts6;
    }

    public void setTs6(long ts6) {
        this.ts6 = ts6;
    }

    public long getTs7() {
        return ts7;
    }

    public void setTs7(long ts7) {
        this.ts7 = ts7;
    }

    public long getTs8() {
        return ts8;
    }

    public void setTs8(long ts8) {
        this.ts8 = ts8;
    }

    public long getTs9() {
        return ts9;
    }

    public void setTs9(long ts9) {
        this.ts9 = ts9;
    }

    public long getTs10() {
        return ts10;
    }

    public void setTs10(long ts10) {
        this.ts10 = ts10;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setNewTimestamp(long ts) {
        if (count == 0) {
            setTs1(ts);
        } else if (count == 1) {
            setTs2(ts);
        } else if (count == 2) {
            setTs3(ts);
        } else if (count == 3) {
            setTs4(ts);
        } else if (count == 4) {
            setTs5(ts);
        } else if (count == 5) {
            setTs6(ts);
        } else if (count == 6) {
            setTs7(ts);
        } else if (count == 7) {
            setTs8(ts);
        } else if (count == 8) {
            setTs9(ts);
        } else if (count == 9) {
            setTs10(ts);
        } else {
            throw new IllegalArgumentException();
        }
        count++;
    }
    @Override
    public String toString() {
        return "count(" + count + ")," +
                ts1 + "," +
                ts2 + "," +
                ts3 + "," +
                ts4 + "," +
                ts5 + "," +
                ts6 + "," +
                ts7 + "," +
                ts8 + "," +
                ts9 + "," +
                ts10;
    }
}
