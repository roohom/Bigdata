package SougouWordCount.NewHourTop3;

import SougouWordCount.EveHourTop3.UserBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: UserGroup
 * @Author: Roohom
 * @Function: 自定义分组比较器 以小时来分组
 * @Date: 2020/8/26 14:52
 * @Software: IntelliJ IDEA
 */
public class UserGroup extends WritableComparator {
    //注册
    public UserGroup() {
        super(SougouWordCount.EveHourTop3.UserBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserBean o1 = (UserBean) a;
        UserBean o2 = (UserBean) b;
        return Integer.valueOf(o1.getHour()).compareTo(o2.getHour());
    }
}
