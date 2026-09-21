//! A hand-rolled Server-Sent Events frame parser.
//!
//! `GET /api/v1/events` (ADR 0043) is a `text/event-stream` per the WHATWG
//! spec, and this crate is deliberately dependency-light — pulling in a whole
//! SSE crate for one endpoint is not worth it, and the wire format is small
//! enough to parse by hand and pin with tests.
//!
//! The parser is byte-oriented on purpose: [`reqwest::Response::chunk`] hands
//! back arbitrary byte slices with no regard for line or even UTF-8 character
//! boundaries, so a frame can be split anywhere — mid-line, mid-terminator, or
//! mid multi-byte character. [`SseParser`] buffers bytes until a full line
//! (and, for a dispatch, a full block) has arrived, decoding each line with
//! [`String::from_utf8_lossy`] so a stray invalid byte degrades to `U+FFFD`
//! rather than panicking or losing the rest of the stream.

/// One dispatched SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseFrame {
    /// The `event:` name, `"message"` when the frame carried none — the
    /// WHATWG default.
    pub(crate) event: String,
    /// The `data:` payload, lines joined with `\n` and no trailing newline.
    pub(crate) data: String,
    /// The `id:` field **as carried by this frame**, not the sticky
    /// last-event-id of the SSE spec. The caller keeps the cursor itself (see
    /// ADR 0043), and a frame that deliberately carries no id — the ADR's
    /// `gap` frame — must not appear to hand one over by inheriting an
    /// earlier frame's id.
    pub(crate) id: Option<String>,
}

/// Parses a `text/event-stream` body fed in as arbitrary byte chunks.
///
/// Push bytes with [`push`](Self::push) as they arrive, then drain
/// [`next_frame`](Self::next_frame) in a loop — one `push` can complete
/// several frames at once, or none. Call [`finish`](Self::finish) once the
/// body ends, to flush a frame that never got its trailing blank line.
#[derive(Debug, Default)]
pub(crate) struct SseParser {
    /// Bytes received but not yet consumed into a line.
    buf: Vec<u8>,
    /// The `event:` field accumulated for the block in progress.
    event_name: Option<String>,
    /// The `data:` field accumulated for the block in progress, lines still
    /// joined with a trailing `\n` each (stripped once on dispatch).
    data: String,
    /// The `id:` field accumulated for the block in progress.
    id: Option<String>,
}

impl SseParser {
    /// A parser with nothing buffered.
    pub(crate) fn new() -> SseParser {
        SseParser::default()
    }

    /// Feed the next chunk of the response body.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// The next complete frame, or `None` when more bytes are needed.
    ///
    /// A single call drains only as many buffered lines as it takes to reach
    /// one dispatchable frame — a comment-only or fields-only block does not
    /// dispatch, so the loop here keeps consuming complete lines past it
    /// rather than stopping short of a frame that is already fully buffered.
    pub(crate) fn next_frame(&mut self) -> Option<SseFrame> {
        loop {
            let line = self.take_line(false)?;
            if line.is_empty() {
                if let Some(frame) = self.dispatch() {
                    return Some(frame);
                }
                // A blank line with an empty data buffer dispatches nothing
                // (per spec); keep going in case a later block is complete.
            } else {
                self.process_field(&line);
            }
        }
    }

    /// Flush at end of body: dispatch a final frame that ended without its
    /// blank line, if any.
    ///
    /// A trailing lone `\r` is ambiguous mid-stream — it might be the first
    /// half of a `\r\n` — but at end of body there is nothing left to
    /// arrive, so it counts as a terminator here.
    pub(crate) fn finish(&mut self) -> Option<SseFrame> {
        loop {
            match self.take_line(true) {
                Some(line) if line.is_empty() => {
                    if let Some(frame) = self.dispatch() {
                        return Some(frame);
                    }
                }
                Some(line) => self.process_field(&line),
                None => break,
            }
        }
        // Anything left has no terminator at all; treat it as one last line
        // rather than dropping it on the floor.
        if !self.buf.is_empty() {
            let line = String::from_utf8_lossy(&self.buf).into_owned();
            self.buf.clear();
            self.process_field(&line);
        }
        self.dispatch()
    }

    /// Pull one terminated line out of the front of the buffer, decoding it
    /// lossily and draining the consumed bytes (including the terminator).
    ///
    /// `force` treats a trailing lone `\r` as a terminator instead of
    /// ambiguous; only [`finish`](Self::finish) sets it.
    fn take_line(&mut self, force: bool) -> Option<String> {
        let (line_end, consumed) = find_line(&self.buf, force)?;
        let line = String::from_utf8_lossy(&self.buf[..line_end]).into_owned();
        self.buf.drain(..consumed);
        Some(line)
    }

    /// Apply one non-blank line to the block in progress.
    fn process_field(&mut self, line: &str) {
        if line.starts_with(':') {
            return; // A comment: ignored entirely.
        }
        let (name, raw_value) = match line.find(':') {
            Some(idx) => (&line[..idx], &line[idx + 1..]),
            None => (line, ""),
        };
        // Exactly one leading space is part of the field syntax, not the
        // value — a second one is real data and stays.
        let value = raw_value.strip_prefix(' ').unwrap_or(raw_value);
        match name {
            "event" => self.event_name = Some(value.to_string()),
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            // The spec says a value containing NUL is ignored outright, not
            // stored and rejected later.
            "id" if !value.contains('\0') => self.id = Some(value.to_string()),
            // `retry`, an `id` carrying NUL, and any field this client does
            // not know are ignored.
            _ => {}
        }
    }

    /// Emit the accumulated block as a frame, unless its data buffer is
    /// empty — per spec, an empty-data block does not dispatch. Either way
    /// the block's state is reset, so nothing leaks into the next frame.
    fn dispatch(&mut self) -> Option<SseFrame> {
        let event = self.event_name.take();
        let id = self.id.take();
        let mut data = std::mem::take(&mut self.data);
        if data.is_empty() {
            return None;
        }
        if data.ends_with('\n') {
            data.pop();
        }
        Some(SseFrame {
            event: event.unwrap_or_else(|| "message".to_string()),
            data,
            id,
        })
    }
}

/// Find the first line terminator in `buf`: `\r\n`, `\n`, or (unless it is an
/// ambiguous trailing byte and `force` is false) a lone `\r`.
///
/// Returns `(line_end, consumed)`: the line is `buf[..line_end]`, and the
/// next line starts at `buf[consumed..]`.
fn find_line(buf: &[u8], force: bool) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        match buf[i] {
            b'\n' => return Some((i, i + 1)),
            b'\r' => {
                return match buf.get(i + 1) {
                    Some(b'\n') => Some((i, i + 2)),
                    Some(_) => Some((i, i + 1)),
                    None if force => Some((i, i + 1)),
                    None => None,
                };
            }
            _ => i += 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(event: &str, data: &str, id: Option<&str>) -> SseFrame {
        SseFrame {
            event: event.to_string(),
            data: data.to_string(),
            id: id.map(|s| s.to_string()),
        }
    }

    /// A complete, single-push frame with `event`, `id` and `data` all parses
    /// as expected, and the single leading space after the colon is
    /// stripped.
    #[test]
    fn a_simple_frame_parses() {
        let mut parser = SseParser::new();
        parser.push(b"event: batch\nid: 7\ndata: {\"a\":1}\n\n");
        assert_eq!(
            parser.next_frame(),
            Some(frame("batch", "{\"a\":1}", Some("7")))
        );
        assert_eq!(parser.next_frame(), None);
    }

    /// No leading space works, and a second leading space is data, not
    /// syntax.
    #[test]
    fn only_one_leading_space_is_stripped_from_a_value() {
        let mut parser = SseParser::new();
        parser.push(b"data:{\"a\":1}\n\n");
        assert_eq!(
            parser.next_frame(),
            Some(frame("message", "{\"a\":1}", None))
        );

        parser.push(b"data:  x\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", " x", None)));
    }

    /// Multiple `data:` lines join with `\n`, and the final trailing newline
    /// is stripped rather than kept.
    #[test]
    fn multiple_data_lines_join_with_newlines() {
        let mut parser = SseParser::new();
        parser.push(b"data: line one\ndata: line two\n\n");
        assert_eq!(
            parser.next_frame(),
            Some(frame("message", "line one\nline two", None))
        );
    }

    /// CRLF, LF, and lone-CR line endings all terminate a line, and a chunk
    /// boundary that falls exactly between a `\r` and its `\n` does not
    /// produce a spurious blank line or an extra dispatch.
    #[test]
    fn all_three_line_ending_styles_work_including_a_split_crlf() {
        let mut crlf = SseParser::new();
        crlf.push(b"data: a\r\n\r\n");
        assert_eq!(crlf.next_frame(), Some(frame("message", "a", None)));

        let mut lf = SseParser::new();
        lf.push(b"data: a\n\n");
        assert_eq!(lf.next_frame(), Some(frame("message", "a", None)));

        // The second lone `\r` is at the very end of the buffer, so it stays
        // ambiguous (it might be the first half of a `\r\n`) until either
        // more bytes arrive or the body ends.
        let mut cr = SseParser::new();
        cr.push(b"data: a\r\r");
        assert_eq!(cr.next_frame(), None);
        assert_eq!(cr.finish(), Some(frame("message", "a", None)));

        // The \r\n terminating the data line, and the \r\n of the blank
        // line, split right down the middle of each CRLF pair.
        let mut split = SseParser::new();
        split.push(b"data: a\r");
        assert_eq!(split.next_frame(), None);
        split.push(b"\n\r");
        assert_eq!(split.next_frame(), None);
        split.push(b"\n");
        assert_eq!(split.next_frame(), Some(frame("message", "a", None)));
        assert_eq!(split.next_frame(), None);
    }

    /// Feeding a multi-frame payload one byte at a time — including
    /// multi-byte UTF-8 in the data — yields exactly the frames that feeding
    /// it whole does.
    #[test]
    fn byte_at_a_time_feeding_matches_whole_payload_feeding() {
        let payload =
            b"event: a\ndata: caf\xc3\xa9\n\nevent: b\ndata: \xf0\x9f\x9a\x80\n\n".as_slice();

        let mut whole = SseParser::new();
        whole.push(payload);
        let mut whole_frames = Vec::new();
        while let Some(f) = whole.next_frame() {
            whole_frames.push(f);
        }

        let mut byte_by_byte = SseParser::new();
        let mut streamed_frames = Vec::new();
        for b in payload {
            byte_by_byte.push(std::slice::from_ref(b));
            while let Some(f) = byte_by_byte.next_frame() {
                streamed_frames.push(f);
            }
        }

        assert_eq!(whole_frames, streamed_frames);
        assert_eq!(
            whole_frames,
            vec![frame("a", "café", None), frame("b", "🚀", None)]
        );
    }

    /// Comment lines are ignored, including one sitting between two real
    /// frames — it must not swallow either.
    #[test]
    fn comment_lines_are_ignored_and_do_not_swallow_neighbouring_frames() {
        let mut parser = SseParser::new();
        parser.push(b": keep-alive\ndata: one\n\n: keep-alive\n\ndata: two\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", "one", None)));
        assert_eq!(parser.next_frame(), Some(frame("message", "two", None)));
        assert_eq!(parser.next_frame(), None);
    }

    /// An unrecognized field name and a `retry:` line are both ignored.
    #[test]
    fn unknown_fields_and_retry_are_ignored() {
        let mut parser = SseParser::new();
        parser.push(b"retry: 5000\nfoo: bar\ndata: x\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", "x", None)));
    }

    /// A frame with no `event:` field defaults to `"message"`.
    #[test]
    fn a_frame_with_no_event_field_defaults_to_message() {
        let mut parser = SseParser::new();
        parser.push(b"data: x\n\n");
        assert_eq!(parser.next_frame().unwrap().event, "message");
    }

    /// A block whose only fields are non-`data` dispatches nothing, but does
    /// not wedge the parser — the next real frame still arrives.
    #[test]
    fn a_block_with_only_non_data_fields_dispatches_nothing_and_does_not_wedge() {
        let mut parser = SseParser::new();
        parser.push(b"id: 5\n\ndata: real\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", "real", None)));
        assert_eq!(parser.next_frame(), None);
    }

    /// An `id` value containing a NUL byte is ignored, leaving the frame's id
    /// unset.
    #[test]
    fn an_id_containing_nul_is_ignored() {
        let mut parser = SseParser::new();
        parser.push(b"id: bad\x00id\ndata: x\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", "x", None)));
    }

    /// `finish()` dispatches a final frame that arrived without its trailing
    /// blank line, and returns `None` when nothing is pending. A trailing
    /// lone `\r` counts as a terminator once `finish()` is called.
    #[test]
    fn finish_flushes_a_frame_missing_its_trailing_blank_line() {
        let mut parser = SseParser::new();
        parser.push(b"event: last\ndata: unfinished\n");
        assert_eq!(parser.next_frame(), None);
        assert_eq!(parser.finish(), Some(frame("last", "unfinished", None)));
        assert_eq!(parser.finish(), None);

        let mut trailing_cr = SseParser::new();
        trailing_cr.push(b"data: x\r");
        assert_eq!(trailing_cr.next_frame(), None);
        assert_eq!(trailing_cr.finish(), Some(frame("message", "x", None)));
    }

    /// Several frames dispatched from a single `push` come out in order
    /// across repeated `next_frame()` calls.
    #[test]
    fn several_frames_from_one_push_come_out_in_order() {
        let mut parser = SseParser::new();
        parser.push(b"data: one\n\ndata: two\n\ndata: three\n\n");
        assert_eq!(parser.next_frame(), Some(frame("message", "one", None)));
        assert_eq!(parser.next_frame(), Some(frame("message", "two", None)));
        assert_eq!(parser.next_frame(), Some(frame("message", "three", None)));
        assert_eq!(parser.next_frame(), None);
    }

    /// State does not leak between frames: an `event:`/`id:` set on one
    /// frame must not appear on the next, which set neither.
    #[test]
    fn state_does_not_leak_between_frames() {
        let mut parser = SseParser::new();
        parser.push(b"event: special\nid: 42\ndata: one\n\ndata: two\n\n");
        assert_eq!(
            parser.next_frame(),
            Some(frame("special", "one", Some("42")))
        );
        assert_eq!(parser.next_frame(), Some(frame("message", "two", None)));
    }
}
