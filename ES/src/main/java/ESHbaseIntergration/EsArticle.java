package ESHbaseIntergration;

import org.apache.hadoop.tracing.SpanReceiverInfo;

/**
 * @ClassName: EsArticle
 * @Author: Roohom
 * @Function: 自定义Java Bean用于封装Excel的内容
 * @Date: 2020/9/29 16:26
 * @Software: IntelliJ IDEA
 */
public class EsArticle {
    private String id;
    private String title;
    private String from;
    private String time;
    private String readCount ;
    private String content;

    public EsArticle() {
    }

    public EsArticle(String id, String title, String from, String time, String readCount, String content) {
        this.id = id;
        this.title = title;
        this.from = from;
        this.time = time;
        this.readCount = readCount;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getReadCount() {
        return readCount;
    }

    public void setReadCount(String readCount) {
        this.readCount = readCount;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return id+"\t"+title+"\t"+from +"\t"+time+"\t"+readCount+"\t"+content;
    }
}
