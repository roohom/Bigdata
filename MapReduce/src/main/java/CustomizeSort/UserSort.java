package CustomizeSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: CustomizeSort.UserSort
 * @Author: Roohom
 * @Function: 自定义排序比较器
 * @Date: 2020/8/24 16:17
 * @Software: IntelliJ IDEA
 */
public class UserSort extends WritableComparator {
    public UserSort(){
        //调用父类的方法来实现注册
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text o1 = (Text) a;
        Text o2 = (Text) b;

        return -o1.compareTo(o2);
    }
}
