package PhoneFlow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: FlowBean
 * @Author: Roohom
 * @Function: 作为map输出的value，封装流量相关的四个值
 * @Date: 2020/8/24 17:00
 * @Software: IntelliJ IDEA
 */
public class FlowBean implements Writable {
    //定义属性
    private int upPack;
    private int downPack;
    private int upFlow;
    private int downFlow;

    //无参构造
    public FlowBean() {
    }

    /**
     * getter and setter
     */
    public void setAll(int upPack, int downPack, int upFlow, int downFlow) {
        this.setUpPack(upPack);
        this.setDownPack(downPack);
        this.setUpFlow(upFlow);
        this.setDownFlow(downFlow);
    }

    public int getUpPack() {
        return upPack;
    }

    public void setUpPack(int upPack) {
        this.upPack = upPack;
    }

    public int getDownPack() {
        return downPack;
    }

    public void setDownPack(int downPack) {
        this.downPack = downPack;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.upPack);
        dataOutput.writeInt(this.downPack);
        dataOutput.writeInt(this.upFlow);
        dataOutput.writeInt(this.downFlow);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upPack = dataInput.readInt();
        this.downPack = dataInput.readInt();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();

    }

    @Override
    public String toString() {
        return this.upPack + "\t" + this.downPack + "\t" + this.upFlow + "\t" + this.downFlow;
    }
}
