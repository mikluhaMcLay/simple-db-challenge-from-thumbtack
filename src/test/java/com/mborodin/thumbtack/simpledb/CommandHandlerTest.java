package com.mborodin.thumbtack.simpledb;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class CommandHandlerTest {
    private CommandHandler handler = new CommandHandler();

    @Test
    public void testGetSetUnset() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("SET ex 10", ""),
                new TestInputOutput("GET ex", "10"),
                new TestInputOutput("UNSET ex", ""),
                new TestInputOutput("GET ex", "NULL"),
                new TestInputOutput("END", "")});
    }

    @Test
    public void testNumEqualTo() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("SET a 10", ""),
                new TestInputOutput("SET b 10", ""),
                new TestInputOutput("NUMEQUALTO 10", "2"),
                new TestInputOutput("NUMEQUALTO 20", "0"),
                new TestInputOutput("SET b 30", ""),
                new TestInputOutput("NUMEQUALTO 10", "1")});
    }

    @Test
    public void testBeginRollback() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("SET a 10", ""),
                new TestInputOutput("GET a", "10"),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("SET a 20", ""),
                new TestInputOutput("GET a", "20"),
                new TestInputOutput("ROLLBACK", ""),
                new TestInputOutput("GET a", "10"),
                new TestInputOutput("ROLLBACK", ""),
                new TestInputOutput("GET a", "NULL")});
    }

    @Test
    public void testBeginCommitRollback() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("SET a 30", ""),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("SET a 40", ""),
                new TestInputOutput("COMMIT", ""),
                new TestInputOutput("GET a", "40"),
                new TestInputOutput("ROLLBACK", "NO TRANSACTION")});
    }

    @Test
    public void testBeginCommitRollback2() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("SET a 50", ""),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("GET a", "50"),
                new TestInputOutput("SET a 60", ""),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("UNSET a", ""),
                new TestInputOutput("GET a", "NULL"),
                new TestInputOutput("ROLLBACK", ""),
                new TestInputOutput("GET a", "60"),
                new TestInputOutput("COMMIT", ""),
                new TestInputOutput("GET a", "60")});
    }

    @Test
    public void testNumEqualToQithCommitRollback() throws Exception {
        test(new TestInputOutput[]{
                new TestInputOutput("SET a 10", ""),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("NUMEQUALTO 10", "1"),
                new TestInputOutput("BEGIN", ""),
                new TestInputOutput("UNSET a", ""),
                new TestInputOutput("NUMEQUALTO 10", "0"),
                new TestInputOutput("ROLLBACK", ""),
                new TestInputOutput("NUMEQUALTO 10", "1"),
                new TestInputOutput("COMMIT", "")
        });
    }

    private void test(TestInputOutput[] tests) throws CommandException {
        for (TestInputOutput test : tests) {
            assertThat(handler.execute(test.input), equalTo(test.output));
        }
    }

    private class TestInputOutput {
        String input, output;

        public TestInputOutput(String input, String output) {
            this.input = input;
            this.output = output;
        }
    }
}
