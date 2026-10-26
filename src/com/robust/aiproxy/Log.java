package com.robust.aiproxy;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/** Minimal single-line console logger built on java.util.logging. No dependencies. */
public final class Log {

    private static final Logger ROOT = Logger.getLogger("robust-ai-proxy");

    static {
        ROOT.setUseParentHandlers(false);
        for (Handler h : ROOT.getHandlers()) {
            h.close();
        }
        ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord r) {
                String level;
                if (r.getLevel() == Level.SEVERE) {
                    level = "ERROR";
                } else if (r.getLevel() == Level.WARNING) {
                    level = "WARN ";
                } else {
                    level = "INFO ";
                }
                String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(r.getMillis()));
                return ts + " " + level + " " + r.getMessage() + System.lineSeparator();
            }
        });
        ROOT.addHandler(ch);
        ROOT.setLevel(Level.INFO);
    }

    private Log() {
    }

    public static void info(String msg, Object... args) {
        ROOT.info(format(msg, args));
    }

    public static void warn(String msg, Object... args) {
        ROOT.warning(format(msg, args));
    }

    public static void error(String msg, Object... args) {
        ROOT.severe(format(msg, args));
    }

    private static String format(String msg, Object... args) {
        if (args == null || args.length == 0) {
            return msg;
        }
        return String.format(msg, args);
    }
}
