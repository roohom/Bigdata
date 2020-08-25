package SecondHouseCount;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: CountBean
 * @Author: Roohom
 * @Function: 自定义java bean 封装[地区 平均价 最高价 最低价]
 * @Date: 2020/8/25 10:01
 * @Software: IntelliJ IDEA
 */
public class CountBean implements WritableComparable<CountBean> {
    private String region;
    private int price;

    public CountBean() {

    }

    public void setAll(String region, int price) {
        setRegion(region);
        setPrice(price);
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.region);
        dataOutput.writeInt(this.price);

    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.region = dataInput.readUTF();
        this.price = dataInput.readInt();

    }

    @Override
    public String toString() {
        return this.getRegion() + "\t" + this.getPrice() + "万";
    }

    /**
     * 自定义排序
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(CountBean o) {
        //先比较地区字段
        int comp = this.getRegion().compareTo(o.getRegion());
        //如果地区字段相同，比较价格字段
        if (comp == 0) {
            //降序排序
            return -Integer.valueOf(this.getPrice()).compareTo(o.getPrice());
        }
        //如果地区字段不同，直接返回
        return comp;
    }
}
