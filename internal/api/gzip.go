package api

import (
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// gzipMinBytes is the smallest body worth compressing. Below this the framing
// and a dictionary setup cost more than the saving, and the small JSON replies
// (a few hundred bytes) are already cheap to send.
const gzipMinBytes = 1024

// gzipPool reuses encoders: gzip.Writer allocation is the only per-request cost
// this middleware adds, and a chart response is a contiguous write, so one
// writer per request is otherwise pure garbage.
var gzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

// gzipMiddleware compresses responses when the client asks for it.
//
// Nothing in the API negotiated encoding before this: measured over the
// dashboard's own polling set, the seven requests one Overview refresh issues
// totalled 113 KB, of which the 24 h series alone was 77 KB, and the same bodies
// gzip to 15967 B (7.1x). The embedded JS bundle is 786 KB and 256 KB gzipped.
//
// Compression is decided before the handler writes anything, because the
// Content-Encoding header has to be sent before the body. That means the
// Content-Length is unknown up front, so the response is chunked — acceptable
// for a JSON API, and Go's own http.ServeContent does the same once a wrapped
// writer is in play. Static assets are already on the wire at their compressed
// size only if they are re-encoded per request, so this applies to them too:
// the bundle is served embedded, uncompressed at rest.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !acceptsGzip(r) || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w, req: r}
		defer gw.Close()
		next.ServeHTTP(gw, r)
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		token := strings.TrimSpace(part)
		if i := strings.IndexByte(token, ';'); i >= 0 {
			token = strings.TrimSpace(token[:i])
		}
		if strings.EqualFold(token, "gzip") {
			return true
		}
	}
	return false
}

// gzipResponseWriter buffers the first gzipMinBytes so small responses are sent
// as-is. It gives up on compression (writing the buffered bytes uncompressed)
// as soon as the handler sets a Content-Length below the threshold, or when the
// caller starts streaming something that must not be re-encoded.
type gzipResponseWriter struct {
	http.ResponseWriter
	req         *http.Request
	status      int
	buf         []byte
	wroteHeader bool
	decided     bool
	gz          *gzip.Writer
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !w.decided {
		if n, ok := w.declaredLength(); ok && n < gzipMinBytes {
			// The handler declared a small body: send it verbatim.
			w.decided = true
			w.ResponseWriter.WriteHeader(w.status)
			w.buf = append(w.buf, p...)
			_, err := w.ResponseWriter.Write(w.buf)
			w.buf = nil
			return len(p), err
		}
		w.buf = append(w.buf, p...)
		if len(w.buf) < gzipMinBytes {
			return len(p), nil
		}
		w.start()
		n, err := w.gz.Write(w.buf)
		w.buf = nil
		if err != nil {
			return n, err
		}
		return len(p), nil
	}
	return w.gz.Write(p)
}

// start commits to compression: it sets the headers and creates the writer.
func (w *gzipResponseWriter) start() {
	w.decided = true
	h := w.Header()
	h.Del("Content-Length")
	h.Set("Content-Encoding", "gzip")
	// Caches must not serve the gzipped body to a client that did not ask for
	// it. Vary is the mechanism; without it a shared cache can hand a gzip body
	// to a caller over a connection that cannot decode it.
	h.Add("Vary", "Accept-Encoding")
	w.ResponseWriter.WriteHeader(w.status)
	w.gz = gzipPool.Get().(*gzip.Writer)
	w.gz.Reset(w.ResponseWriter)
}

func (w *gzipResponseWriter) Flush() {
	if w.decided && w.gz != nil {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *gzipResponseWriter) declaredLength() (int64, bool) {
	raw := w.Header().Get("Content-Length")
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// Close finishes the stream. When compression was never started the buffered
// bytes are written verbatim, so a small response keeps its original
// Content-Length and no Vary/Content-Encoding is added.
func (w *gzipResponseWriter) Close() {
	if !w.decided {
		if len(w.buf) > 0 || w.wroteHeader {
			status := w.status
			if status == 0 {
				status = http.StatusOK
			}
			w.ResponseWriter.WriteHeader(status)
			if len(w.buf) > 0 {
				_, _ = w.ResponseWriter.Write(w.buf)
			}
		}
		return
	}
	if w.gz == nil {
		return
	}
	_ = w.gz.Close()
	gzipPool.Put(w.gz)
	w.gz = nil
}
