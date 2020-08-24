package SecondOrder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: CustomizeSort
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/25 00:14
 * @Software: IntelliJ IDEA
 */
public class CustomizeSort extends WritableComparator {
    public CustomizeSort() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text o1 = (Text) a;
        Text o2 = (Text) b;

        return -o1.compareTo(o2);
    }
}
