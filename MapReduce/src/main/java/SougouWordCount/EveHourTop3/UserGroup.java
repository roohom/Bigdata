package SougouWordCount.EveHourTop3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: UserGroup
 * @Author: Roohom
 * @Function: 自定义分组器
 * @Date: 2020/8/26 14:52
 * @Software: IntelliJ IDEA
 */
public class UserGroup extends WritableComparator {
    //注册
    public UserGroup() {
        super(UserBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserBean o1 = (UserBean) a;
        UserBean o2 = (UserBean) b;
        return Integer.valueOf(o1.getHour()).compareTo(o2.getHour());

    }
}
