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
}
