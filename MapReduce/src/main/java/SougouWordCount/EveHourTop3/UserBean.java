package SougouWordCount.EveHourTop3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: UserBean
 * @Author: Roohom
 * @Function: 自定义java bean封装 小时 搜索量 搜索词 三个字段
 * @Date: 2020/8/26 00:45
 * @Software: IntelliJ IDEA
 */
public class UserBean implements WritableComparable<UserBean> {
    private int hour;
    private int count;
    private String searchItem;


    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getSearchItem() {
        return searchItem;
    }

    public void setSearchItem(String searchItem) {
        this.searchItem = searchItem;
    }

    /**
     * 先按时间排序 再按搜索量排序
     *
     * @param o 自定义userbean对象
     * @return 返回是否相同的标志
     */
    @Override
    public int compareTo(UserBean o) {
        //先比较时间
        int comp = Integer.compare(this.getHour(), o.getHour());
        //如果时间相同，比较搜索量
        if (comp == 0) {
            int comp2 = Integer.valueOf(this.getCount()).compareTo(o.getCount());
            //比较搜索词
            //降序排序
            if (comp2==0)
            {
                return -this.getSearchItem().compareTo(o.getSearchItem());
            }
            return -comp2;
        }
        //如果访问时间不同，直接返回
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(hour);
        dataOutput.writeInt(count);
        dataOutput.writeUTF(searchItem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hour = dataInput.readInt();
        this.count = dataInput.readInt();
        this.searchItem = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.getHour() + "\t" + this.getCount() + "\t" + this.getSearchItem();
    }

}
