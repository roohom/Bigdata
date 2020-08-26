import java.util.Comparator;
import java.util.TreeSet;

/**
 * @ClassName: Text
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/26 16:59
 * @Software: IntelliJ IDEA
 */
public class Test {
    public static void main(String[] args) {
        TreeSet<Integer> testSet = new TreeSet<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return Integer.valueOf(o2).compareTo(o1);
            }
        });
        testSet.add(12);
        testSet.add(10);
        testSet.add(15);
        testSet.add(1);
        testSet.add(30);
        for (Integer integer : testSet) {
            System.out.println(integer);
        }
    }
}
