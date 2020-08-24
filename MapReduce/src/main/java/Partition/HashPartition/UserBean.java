package Partition.HashPartition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: UserBean
 * @Author: Roohom
 * @Function: 自定义数据类型 实现write和readFields
 * @Date: 2020/8/24 14:55
 * @Software: IntelliJ IDEA
 */
public class UserBean implements Writable {

    //定义属性
    private String word;
    private int len;

    public UserBean() {
    }

    public UserBean(String word, int len) {
        this.word = word;
        this.len = len;
    }

    public void setAll(String word, int len) {
        this.setWord(word);
        this.setLen(len);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    //实现序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.word);
        dataOutput.writeInt(this.len);
    }

    //实现反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.len = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.word + "\t" + this.len;
    }
}
