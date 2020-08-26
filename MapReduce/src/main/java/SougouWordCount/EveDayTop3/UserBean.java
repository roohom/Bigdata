package SougouWordCount.EveDayTop3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: UserBean
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/26 00:45
 * @Software: IntelliJ IDEA
 */
public class UserBean implements WritableComparable<UserBean> {
    private int count;
    private String searchItem;


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

    @Override
    public int compareTo(UserBean o) {
        //先比较访问次数
        int comp = Integer.compare(this.getCount(), o.getCount());
        //如果访问次数相同，比较搜索词字段
        if (comp == 0) {
            //降序排序
            return -this.getSearchItem().compareTo(o.getSearchItem());
        }
        //如果访问次数不同，直接返回
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeUTF(searchItem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.searchItem = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.getCount() +"\t" + this.getSearchItem();
    }

}
