package com.mborodin.thumbtack.simpledb;

public class CommandHandler {
    private final DB<String, String> db = SimpleDB.INSTANCE;

    public String execute(String command) throws CommandException {
        String response = "";
        String[] split = command.split(" ");
        switch (split[0].toUpperCase()) {
            case "GET":
                if (split.length != 2) {
                    throw new CommandException("Expected 'GET key', got: " + command);
                }
                response = db.get(split[1]);
                if (response == null) {
                    response = "NULL";
                }
                break;
            case "SET":
                if (split.length != 3) {
                    throw new CommandException("Expected 'GET key value', got: " + command);
                }
                db.set(split[1], split[2]);
                break;
            case "UNSET":
                if (split.length != 2) {
                    throw new CommandException("Expected 'UNSET key', got: " + command);
                }
                db.unset(split[1]);
                break;
            case "NUMEQUALTO":
                if (split.length != 2) {
                    throw new CommandException("Expected 'NUMEQUALTO key', got: " + command);
                }
                response = String.valueOf(db.countByValue(split[1]));
                break;
            case "COMMIT":
                try {
                    db.commit();
                } catch (NoTransactionException e) {
                    response = "NO TRANSACTION";
                }
                break;
            case "ROLLBACK":
                try {
                    db.rollback();
                } catch (NoTransactionException e) {
                    response = "NO TRANSACTION";
                }
                break;
            case "BEGIN":
                db.begin();
                break;
            default:
                throw new CommandException("Unsupported command: " + command);
        }
        return response;
    }
}
