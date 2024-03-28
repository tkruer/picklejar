package org.picklejar.session;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

class Command {
  private List<String> args;
  private Socket conn;
  private static final Logger logger = Logger.getLogger(Command.class.getName());

  public Command(List<String> args, Socket conn) {
    this.args = args;
    this.conn = conn;
  }

  public boolean handle(ConcurrentHashMap<String, String> cache) {
    // Implement command handling logic here
    switch (args.get(0).toUpperCase()) {
      case "SET":
        logger.log(Level.INFO, "SET command received");
        logger.log(Level.INFO, "Key: " + args.get(1) + ", Value: " + args.get(2));
        return set(cache);
      // Add other cases for different commands
      default:
        // Command not supported
        return true;
    }
  }

  private boolean set(ConcurrentHashMap<String, String> cache) {
    if (args.size() < 3) {
      // Error: wrong number of arguments
      return true;
    }
    String key = args.get(1);
    String value = args.get(2);
    cache.put(key, value);

    // Handle response to client
    // ...

    return true;
  }

  // Additional command methods (e.g., get, del, etc.) can be added here
}

class Parser {
  private Socket conn;
  private BufferedReader reader;
  private StringBuilder line;
  private int pos;

  public Parser(Socket conn) throws IOException {
    this.conn = conn;
    this.reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    this.line = new StringBuilder();
    this.pos = 0;
  }

  public Command command() throws IOException {
    line = new StringBuilder(reader.readLine());
    pos = 0;
    List<String> args = new ArrayList<>();

    while (!atEnd()) {
      skipWhitespace();
      if (current() == '"') {
        args.add(consumeQuotedString());
      } else {
        args.add(consumeUnquotedString());
      }
    }

    return new Command(args, conn);
  }

  private void skipWhitespace() {
    while (!atEnd() && Character.isWhitespace(current())) {
      advance();
    }
  }

  private String consumeQuotedString() {
    advance(); // Skip the opening quote
    StringBuilder sb = new StringBuilder();
    while (!atEnd() && current() != '"') {
      sb.append(current());
      advance();
    }
    advance(); // Skip the closing quote
    return sb.toString();
  }

  private String consumeUnquotedString() {
    StringBuilder sb = new StringBuilder();
    while (!atEnd() && !Character.isWhitespace(current())) {
      sb.append(current());
      advance();
    }
    return sb.toString();
  }

  private char current() {
    if (atEnd()) {
      return '\0';
    }
    return line.charAt(pos);
  }

  private void advance() {
    pos++;
  }

  private boolean atEnd() {
    return pos >= line.length();
  }
}

public class Session {
  private static final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
  private static final Logger logger = Logger.getLogger(Session.class.getName());

  public static void startSession(Socket conn) {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()))) {
      Parser parser = new Parser(conn);

      while (true) {
        try {
          Command cmd = parser.command();
          if (cmd != null) {
            boolean shouldContinue = cmd.handle(cache);
            // Assume the command handle method itself sends the appropriate response.
            // If handle method doesn't send response, send it here:
            writer.write("Response to " + cmd.toString() + "\r\n");
            writer.flush();
            if (!shouldContinue) {
              break;
            }
          }
        } catch (IOException e) {
          logger.log(Level.SEVERE, "Error processing command", e);
          writer.write("-ERR " + e.getMessage() + "\r\n");
          writer.flush();
          break;
        }
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error handling session", e);
    } finally {
      try {
        logger.log(Level.INFO, "Closing connection: " + conn);
        conn.close();
      } catch (IOException e) {
        logger.log(Level.SEVERE, "Error closing connection", e);
      }
    }
  }
}
