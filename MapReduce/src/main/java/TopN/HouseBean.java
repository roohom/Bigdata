package TopN;

/**
 * @ClassName: HouseBean
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/26 17:18
 * @Software: IntelliJ IDEA
 */
public class HouseBean {
    private int highest;
    private int lowest;
    private int avg;
    private int total;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getHighest() {
        return highest;
    }

    public void setHighest(int highest) {
        this.highest = highest;
    }

    public int getLowest() {
        return lowest;
    }

    public void setLowest(int lowest) {
        this.lowest = lowest;
    }

    public int getAvg() {
        return avg;
    }

    public void setAvg(int avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return this.getTotal() + "\t" + this.getAvg() + "\t" + this.getHighest() + "\t" + this.getLowest();
    }
}
