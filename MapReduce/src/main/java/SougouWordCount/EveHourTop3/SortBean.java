package SougouWordCount.EveHourTop3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: SortBean
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/26 20:03
 * @Software: IntelliJ IDEA
 */
public class SortBean implements WritableComparable<SortBean> {

    private int searchCount;
    private String searchItem;


    public int getSearchCount() {
        return searchCount;
    }

    public void setSearchCount(int searchCount) {
        this.searchCount = searchCount;
    }

    public String getSearchItem() {
        return searchItem;
    }

    public void setSearchItem(String searchItem) {
        this.searchItem = searchItem;
    }

    @Override
    public int compareTo(SortBean o) {
        //先比较搜索量
        int comp = Integer.valueOf(this.getSearchCount()).compareTo(o.getSearchCount());
        //如果搜索量相同，比较搜索词
        if (comp == 0) {
            //比较搜索词
            return -this.getSearchItem().compareTo(o.getSearchItem());
        }
        //如果搜索量不同，直接返回
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.searchCount);
        dataOutput.writeUTF(this.searchItem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.searchCount = dataInput.readInt();
        this.searchItem = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.getSearchCount()+ "\t" +this.getSearchItem();
    }

}
