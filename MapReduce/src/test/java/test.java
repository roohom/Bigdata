/**
 * @ClassName: test
 * @Author: Roohom
 * @Function:
 * @Date: 2020/8/31 23:06
 * @Software: IntelliJ IDEA
 */
public class test {
    public static void main(String[] args) {
        int number = 100;
        System.out.println(number);
        change(number);
        System.out.println(number);
    }

    public static void change(int number)
    {
        number=200;
    }
}
