package com.mborodin.thumbtack.simpledb;

import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class SimpleDBTest {
    private SimpleDB<String, String> simpleDB = SimpleDB.INSTANCE;

    @After
    public void tearDown() throws Exception {
        simpleDB.restart();
    }

    @Test
    public void testSetGet() throws Exception {
        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value-2");
        simpleDB.set("1", "new value");

        assertThat(simpleDB.get("1"), equalTo("new value"));
        assertThat(simpleDB.get("2"), equalTo("value-2"));
        assertThat(simpleDB.get("3"), nullValue());
    }

    @Test
    public void testUnset() throws Exception {
        assertThat(simpleDB.get("1"), nullValue());
        simpleDB.unset("1");
        assertThat(simpleDB.get("1"), nullValue());

        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value-2");
        simpleDB.unset("2");
        assertThat(simpleDB.get("1"), equalTo("value-1"));
        assertThat(simpleDB.get("2"), nullValue());
    }

    @Test
    public void testCountByValue() throws Exception {
        assertThat(simpleDB.countByValue("value"), equalTo(0L));
        simpleDB.set("1", "value");
        simpleDB.set("2", "value-2");
        assertThat(simpleDB.countByValue("value"), equalTo(1L));
        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value");
        assertThat(simpleDB.countByValue("value"), equalTo(1L));
        simpleDB.set("2", "value-1");
        assertThat(simpleDB.countByValue("value"), equalTo(0L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(2L));
    }

    @Test
    public void testGetSetInTransaction() throws Exception {
        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value-2");

        simpleDB.begin();
        assertThat(simpleDB.get("1"), equalTo("value-1"));
        assertThat(simpleDB.get("2"), equalTo("value-2"));
        assertThat(simpleDB.get("3"), nullValue());
        simpleDB.set("3", "value-3");
        simpleDB.set("2", "new value-2");
        assertThat(simpleDB.get("1"), equalTo("value-1"));
        assertThat(simpleDB.get("2"), equalTo("new value-2"));
        assertThat(simpleDB.get("3"), equalTo("value-3"));
    }

    @Test
    public void testRollback() throws Exception {
        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value-2");

        // transaction 1
        simpleDB.begin();
        simpleDB.set("1", "tr-1-value-1");
        simpleDB.set("2", "tr-1-value-2");
        simpleDB.set("3", "VALUE");

        assertThat(simpleDB.get("1"), equalTo("tr-1-value-1"));
        assertThat(simpleDB.get("2"), equalTo("tr-1-value-2"));
        assertThat(simpleDB.get("3"), equalTo("VALUE"));
        assertThat(simpleDB.countByValue("VALUE"), equalTo(1L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(0L));

        // transaction 2
        simpleDB.begin();
        simpleDB.set("1", "tr-2-value-1");
        simpleDB.set("2", "VALUE");
        simpleDB.set("4", "VALUE");
        simpleDB.unset("3");

        assertThat(simpleDB.get("1"), equalTo("tr-2-value-1"));
        assertThat(simpleDB.get("2"), equalTo("VALUE"));
        assertThat(simpleDB.get("3"), nullValue());
        assertThat(simpleDB.get("4"), equalTo("VALUE"));
        assertThat(simpleDB.countByValue("VALUE"), equalTo(2L));
        assertThat(simpleDB.countByValue("tr-2-value-1"), equalTo(1L));

        // transaction 1
        simpleDB.rollback();
        assertThat(simpleDB.get("1"), equalTo("tr-1-value-1"));
        assertThat(simpleDB.get("2"), equalTo("tr-1-value-2"));
        assertThat(simpleDB.get("3"), equalTo("VALUE"));
        assertThat(simpleDB.get("4"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(1L));
        assertThat(simpleDB.countByValue("tr-2-value-1"), equalTo(0L));
        assertThat(simpleDB.countByValue("tr-1-value-1"), equalTo(1L));

        // transaction 3
        simpleDB.begin();
        simpleDB.unset("1");
        simpleDB.unset("2");
        simpleDB.unset("3");
        simpleDB.unset("4");
        simpleDB.unset("5");
        assertThat(simpleDB.get("1"), nullValue());
        assertThat(simpleDB.get("2"), nullValue());
        assertThat(simpleDB.get("3"), nullValue());
        assertThat(simpleDB.get("4"), nullValue());
        assertThat(simpleDB.get("5"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(0L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(0L));
        assertThat(simpleDB.countByValue("tr-1-value-1"), equalTo(0L));

        // transaction 1
        simpleDB.rollback();
        assertThat(simpleDB.get("1"), equalTo("tr-1-value-1"));
        assertThat(simpleDB.get("2"), equalTo("tr-1-value-2"));
        assertThat(simpleDB.get("3"), equalTo("VALUE"));
        assertThat(simpleDB.get("4"), nullValue());
        assertThat(simpleDB.get("5"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(1L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(0L));
        assertThat(simpleDB.countByValue("tr-1-value-1"), equalTo(1L));

        // before transactions
        simpleDB.rollback();
        assertThat(simpleDB.get("1"), equalTo("value-1"));
        assertThat(simpleDB.get("2"), equalTo("value-2"));
        assertThat(simpleDB.get("3"), nullValue());
        assertThat(simpleDB.get("4"), nullValue());
        assertThat(simpleDB.get("5"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(0L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(1L));
        assertThat(simpleDB.countByValue("value-2"), equalTo(1L));
        assertThat(simpleDB.countByValue("tr-1-value-1"), equalTo(0L));
    }

    @Test(expected = NoTransactionException.class)
    public void testRollbackThrowsExceptionWhenNoOpenedTransaction() throws Exception {
        simpleDB.rollback();
    }

    @Test(expected = NoTransactionException.class)
    public void testCommitThrowsExceptionWhenNoOpenedTransaction() throws Exception {
        simpleDB.commit();
    }

    @Test
    public void testCommitOneTransaction() throws Exception {
        simpleDB.set("0", "value-0");
        simpleDB.set("1", "value-1");
        simpleDB.set("2", "value-2");
        simpleDB.set("3", "value-3");
        simpleDB.set("4", "value-4");

        // transaction 1
        simpleDB.begin();
        simpleDB.set("1", "VALUE");
        simpleDB.set("2", "VALUE");
        simpleDB.set("5", "VALUE");
        simpleDB.unset("2");
        simpleDB.unset("4");
        simpleDB.unset("6");

        simpleDB.commit();
        assertThat(simpleDB.get("0"), equalTo("value-0"));
        assertThat(simpleDB.get("1"), equalTo("VALUE"));
        assertThat(simpleDB.get("2"), nullValue());
        assertThat(simpleDB.get("3"), equalTo("value-3"));
        assertThat(simpleDB.get("4"), nullValue());
        assertThat(simpleDB.get("5"), equalTo("VALUE"));
        assertThat(simpleDB.get("6"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(2L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(0L));
        assertThat(simpleDB.countByValue("value-3"), equalTo(1L));
    }

    @Test
    public void testCommitMultipleTransactions() throws Exception {
        simpleDB.set("0", "value-0");
        simpleDB.set("1", "value-1");

        simpleDB.begin();
        simpleDB.begin();
        simpleDB.set("1", "VALUE");
        simpleDB.set("3", "VALUE");
        simpleDB.unset("2");

        simpleDB.begin();
        simpleDB.set("1", "VALUE");
        simpleDB.set("2", "VALUE");
        simpleDB.unset("0");

        simpleDB.begin();
        simpleDB.set("0", "VALUE");
        simpleDB.set("1", "VALUE-1");
        simpleDB.set("2", "VALUE");
        simpleDB.unset("3");

        simpleDB.commit();
        assertThat(simpleDB.get("0"), equalTo("VALUE"));
        assertThat(simpleDB.get("1"), equalTo("VALUE-1"));
        assertThat(simpleDB.get("2"), equalTo("VALUE"));
        assertThat(simpleDB.get("3"), nullValue());
        assertThat(simpleDB.countByValue("VALUE"), equalTo(2L));
        assertThat(simpleDB.countByValue("VALUE-1"), equalTo(1L));
        assertThat(simpleDB.countByValue("value-1"), equalTo(0L));
    }
}
