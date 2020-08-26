package SougouWordCount.EveDayTop3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: UserSort
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/26 00:12
 * @Software: IntelliJ IDEA
 */
public class UserSort extends WritableComparator {
    public UserSort() {
        super(UserBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserBean o1 = (UserBean) a;
        UserBean o2 = (UserBean) b;
        return  String.valueOf(o1.getCount()).compareTo(String.valueOf(o2.getCount()));
    }
}
