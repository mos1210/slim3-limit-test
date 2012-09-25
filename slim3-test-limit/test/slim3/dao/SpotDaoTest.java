package slim3.dao;

import org.slim3.tester.AppEngineTestCase;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class SpotDaoTest extends AppEngineTestCase {

    private SpotDao dao = new SpotDao();

    @Test
    public void test() throws Exception {
        assertThat(dao, is(notNullValue()));
    }
}
