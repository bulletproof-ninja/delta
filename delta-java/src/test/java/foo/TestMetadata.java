package foo;

import java.time.ZonedDateTime;

import org.junit.Test;
import static org.junit.Assert.*;

import delta.java.MetadataBean;

public class TestMetadata {

    static class SomeMetadata implements MetadataBean {
        private final long userId;
        private final ZonedDateTime timestamp;
        SomeMetadata(long userId) {
            this.userId = userId;
            this.timestamp = ZonedDateTime.now();
        }
        public long getUserId() {
            return userId;
        }
        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

    }

    @Test
    public void map() {
        SomeMetadata md = new SomeMetadata(42L);
        scala.collection.Map<String, String> map = md.toMap();
        assertEquals("42", map.apply("userId"));
        assertEquals(md.getTimestamp().toString(), map.apply("timestamp"));
    }

}
