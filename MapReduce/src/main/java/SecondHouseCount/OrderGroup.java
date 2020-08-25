package SecondHouseCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: OrderGroup
 * @Author: Roohom
 * @Function: 自定义分组比较器
 * @Date: 2020/8/25 18:04
 * @Software: IntelliJ IDEA
 */
public class OrderGroup extends WritableComparator {
    //注册
    public OrderGroup()
    {
        super(CountBean.class,true);
    }

    //重写compareTo
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CountBean o1 = (CountBean) a;
        CountBean o2 = (CountBean) b;
        return -o1.getRegion().compareTo(o2.getRegion());
    }
}
