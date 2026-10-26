package com.robust.aiproxy;

import java.nio.charset.StandardCharsets;

/** Helpers for recognizing OpenAI-style requests and Server-Sent Events. */
public final class SseUtil {

    private SseUtil() {
    }

    /**
     * Best-effort detection of the "stream" flag in a JSON request body.
     * Scans for the first "stream" member and reads its boolean value.
     * Returns false for non-JSON bodies or when the flag is absent
     * (OpenAI defaults "stream" to false).
     */
    public static boolean isStreamingRequest(byte[] body) {
        if (body == null || body.length == 0) {
            return false;
        }
        String s = new String(body, StandardCharsets.UTF_8);
        int i = s.indexOf("\"stream\"");
        while (i >= 0) {
            int j = i + 8;
            while (j < s.length() && Character.isWhitespace(s.charAt(j))) {
                j++;
            }
            if (j < s.length() && s.charAt(j) == ':') {
                j++;
                while (j < s.length() && Character.isWhitespace(s.charAt(j))) {
                    j++;
                }
                if (s.startsWith("true", j)) {
                    return true;
                }
                if (s.startsWith("false", j)) {
                    return false; // explicit false: stop scanning
                }
            }
            i = s.indexOf("\"stream\"", i + 1);
        }
        return false;
    }

    /**
     * True when the most recent "data:" line in the buffer is "[DONE]",
     * which is how OpenAI-style streams terminate. Lines are scanned from
     * the tail; trailing blank lines are skipped.
     */
    public static boolean endsWithDone(byte[] buf) {
        if (buf == null || buf.length == 0) {
            return false;
        }
        int lineEnd = buf.length;
        for (int back = 0; back < 16; back++) {
            int nl = -1;
            for (int i = lineEnd - 1; i >= 0; i--) {
                if (buf[i] == '\n') {
                    nl = i;
                    break;
                }
            }
            int lineStart = (nl == -1) ? 0 : nl + 1;
            if (lineEnd > lineStart) {
                String line = new String(buf, lineStart, lineEnd - lineStart, StandardCharsets.UTF_8).trim();
                if (line.startsWith("data:")) {
                    return line.substring(5).trim().equals("[DONE]");
                }
            }
            if (nl == -1) {
                break;
            }
            lineEnd = nl; // next iteration scans strictly left of this newline
        }
        return false;
    }

    /** True when the first non-blank line of the buffer is an SSE field line. */
    public static boolean looksLikeSse(byte[] buf) {
        if (buf == null || buf.length == 0) {
            return false;
        }
        int i = 0;
        while (i < buf.length && (buf[i] == '\n' || buf[i] == '\r' || buf[i] == ' ')) {
            i++;
        }
        int start = i;
        while (i < buf.length && buf[i] != '\n') {
            i++;
        }
        if (start >= buf.length) {
            return false;
        }
        String line = new String(buf, start, i - start, StandardCharsets.UTF_8);
        return line.startsWith("data:") || line.startsWith("event:") || line.startsWith("id:") || line.startsWith(":");
    }
}
