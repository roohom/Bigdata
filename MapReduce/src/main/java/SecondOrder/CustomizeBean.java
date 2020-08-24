package SecondOrder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: CustomizeBean
 * @Author: Roohom
 * @Function: 自定义bean实现writableComparable接口实现排序器
 * @Date: 2020/8/25 00:21
 * @Software: IntelliJ IDEA
 */
public class CustomizeBean implements WritableComparable<CustomizeBean> {
    private String keyWord;
    private int valueInt;

    public CustomizeBean() {

    }

    public void setAll(String keyWord, int valueInt) {
        this.setKeyWord(keyWord);
        this.setValueInt(valueInt);
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public int getValueInt() {
        return valueInt;
    }

    public void setValueInt(int valueInt) {
        this.valueInt = valueInt;
    }

    public int compareTo(CustomizeBean o) {
        //先比较第一个值是否相等，如果不相等，以第一个值的大小作为结果
        int comp = this.getKeyWord().compareTo(o.getKeyWord());
        //如果相等,以第二个值的结果作为最后的结果
        if(0 == comp){
            return Integer.valueOf(this.getValueInt()).compareTo(Integer.valueOf(o.getValueInt()));
        }
        return comp;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.keyWord);
        dataOutput.writeInt(this.valueInt);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.keyWord = dataInput.readUTF();
        this.valueInt = dataInput.readInt();
    }
}
