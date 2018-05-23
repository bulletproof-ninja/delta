package foo;

import java.time.ZonedDateTime;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

import delta.java.MetadataBean;
import delta.java.ScalaHelp;

public class TestMetadata {

    static interface SomeMetadata extends MetadataBean {
        public long getUserId();
        public ZonedDateTime getTimestamp();
    }

    static class SomeMetadataImpl implements SomeMetadata {
        private final long userId;
        private final ZonedDateTime timestamp;
        SomeMetadataImpl(long userId) {
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
        SomeMetadata md = new SomeMetadataImpl(42L);
        scala.collection.Map<String, String> map = md.toMap();
        assertEquals("42", map.apply("userId"));
        assertEquals(md.getTimestamp().toString(), map.apply("timestamp"));
    }

    @Test
    public void fromMap() {
        SomeMetadata mdImpl = new SomeMetadataImpl(42L);
        Map<String, String> map = ScalaHelp.asJava(mdImpl.toMap());
        SomeMetadata mdProxy = MetadataBean.fromMap(map, SomeMetadata.class);
        assertEquals(mdImpl.getUserId(), mdProxy.getUserId());
        assertEquals(mdImpl.getTimestamp(), mdProxy.getTimestamp());
    }
}
