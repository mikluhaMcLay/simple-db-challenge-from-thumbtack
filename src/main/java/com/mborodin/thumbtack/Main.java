package com.mborodin.thumbtack;

import com.mborodin.thumbtack.simpledb.CommandException;
import com.mborodin.thumbtack.simpledb.CommandHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        CommandHandler ch = new CommandHandler();

        String command;
        while ((command = in.readLine()) != null) {
            if ("END".equalsIgnoreCase(command)) {
                return;
            }
            String response = "";
            try {
                response = ch.execute(command);
            } catch (CommandException e) {
                e.printStackTrace();
            }
            if (!response.isEmpty()) {
                System.out.println(response);
            }
        }
    }
}
