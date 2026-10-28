package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// compactCapGateway serves the real loopback stack and counts only compactions,
// leaving background usage and model reads out of the count.
func compactCapGateway(t *testing.T) (*atomic.Int32, func(string, string, string) (int, string)) {
	t.Helper()
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/codex/responses/compact" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		io.WriteString(w, `{"output":[{"type":"compaction","encrypted_content":"gAAAAB-sealed"}]}`)
	}))
	t.Cleanup(up.Close)
	was, wasAPI := provider.CodexBase, codexAPIBase
	provider.CodexBase = up.URL + CodexPath
	codexAPIBase = up.URL + CodexPath
	t.Cleanup(func() { provider.CodexBase, codexAPIBase = was, wasAPI })
	gw := httptest.NewServer(lanGuard(New().Handler()))
	t.Cleanup(gw.Close)
	post := func(ask, token, account string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, gw.URL+CodexPath+"/responses/compact", strings.NewReader(ask))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("chatgpt-account-id", account)
		res, err := gw.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, string(body)
	}
	return &calls, post
}

const compactCapBody = `{"model":"gpt-5.5","input":[{"role":"user","content":"ping"}]}`

// A native compaction spends the signed-in account too: its cap holds
// /responses/compact just as it holds /responses, even with only one account.
func TestCodexNativeCompactAccountCap(t *testing.T) {
	codexSignedIn(t)
	capUsage(t, map[string]float64{"me@example.com": 95})
	if err := provider.SetAccountCap("codex", "me@example.com", 95); err != nil {
		t.Fatal(err)
	}
	calls, post := compactCapGateway(t)
	code, body := post(compactCapBody, "chatgpt-token", "acct-1")
	if code != http.StatusTooManyRequests || calls.Load() != 0 || !strings.Contains(body, "rate_limit_error") || !strings.Contains(body, "95%") || !strings.Contains(body, "me@example.com") {
		t.Fatalf("at cap: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
	// A local hold on ChatGPT leaves a third-party model available.
	third := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, nativeSealedReply("gAAAAB-third-party"))
	}))
	t.Cleanup(third.Close)
	if err := provider.Save(provider.Provider{ID: "third", Name: "Third", Key: "test-key", Models: []string{"m1"}, Responses: third.URL}); err != nil {
		t.Fatal(err)
	}
	if code, body := codexPost(t, `{"model":"third/m1","stream":true,"input":"ping"}`); code != 200 || !strings.Contains(body, "gAAAAB-third-party") || calls.Load() != 0 {
		t.Fatalf("third party: status %d, ChatGPT calls %d: %s", code, calls.Load(), body)
	}
	if err := provider.SetAccountCap("codex", "me@example.com", 0); err != nil {
		t.Fatal(err)
	}
	if code, body := post(compactCapBody, "chatgpt-token", "acct-1"); code != http.StatusOK || calls.Load() != 1 || !strings.Contains(body, "gAAAAB-sealed") {
		t.Fatalf("cap lifted: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
}

func TestCodexNativeCompactCapBoundaries(t *testing.T) {
	for _, c := range []struct {
		name, token, account string
		used                 [2]float64
		cap                  int
		status               int
	}{
		{"below", "chatgpt-token", "acct-1", [2]float64{94.9, 10}, 95, 200},
		{"weekly", "chatgpt-token", "acct-1", [2]float64{10, 95}, 95, 429},
		{"no cap", "chatgpt-token", "acct-1", [2]float64{95, 95}, 0, 200},
		{"API key", "sk-test", "acct-1", [2]float64{95, 95}, 95, 200},
		{"other sign-in", "chatgpt-token", "acct-elsewhere", [2]float64{95, 95}, 95, 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			codexSignedIn(t)
			windowUsage(t, map[string][2]float64{"me@example.com": c.used})
			if err := provider.SetAccountCap("codex", "me@example.com", c.cap); err != nil {
				t.Fatal(err)
			}
			calls, post := compactCapGateway(t)
			code, body := post(compactCapBody, c.token, c.account)
			wantCalls := int32(1)
			if c.status == 429 {
				wantCalls = 0
			}
			if code != c.status || calls.Load() != wantCalls {
				t.Fatalf("status %d, upstream calls %d: %s", code, calls.Load(), body)
			}
		})
	}
}

// Both a paused sign-in and an earlier seal can put compaction on a spare.
// It is that spare's cap that holds the call, never the sign-in's instead.
func TestCodexNativeCompactCapsTheActualAccount(t *testing.T) {
	for _, mode := range []string{"paused", "sealed"} {
		for _, used := range []float64{20, 95} {
			t.Run(fmt.Sprintf("%s/%g", mode, used), func(t *testing.T) {
				codexSignedIn(t, "spare@example.com")
				capUsage(t, map[string]float64{"me@example.com": 95, "spare@example.com": used})
				for _, user := range []string{"me@example.com", "spare@example.com"} {
					if err := provider.SetAccountCap("codex", user, 95); err != nil {
						t.Fatal(err)
					}
				}
				body := compactCapBody
				if mode == "paused" {
					if err := provider.SetLoginOn("codex", "me@example.com", false); err != nil {
						t.Fatal(err)
					}
				} else {
					wroteCheckpoint("gAAAAB-spare", candidate{p: provider.Provider{ID: "codex", Account: &provider.Account{User: "spare@example.com"}}})
					body = `{"model":"gpt-5.5","input":[{"type":"compaction","encrypted_content":"gAAAAB-spare"}]}`
				}
				calls, post := compactCapGateway(t)
				code, reply := post(body, "chatgpt-token", "acct-1")
				if used == 95 {
					if code != 429 || calls.Load() != 0 || !strings.Contains(reply, "spare@example.com") {
						t.Fatalf("spare capped: status %d, upstream calls %d: %s", code, calls.Load(), reply)
					}
				} else if code != 200 || calls.Load() != 1 {
					t.Fatalf("spare has room: status %d, upstream calls %d: %s", code, calls.Load(), reply)
				}
			})
		}
	}
}

func TestCodexNativeCompactCarefulCap(t *testing.T) {
	codexSignedIn(t)
	capUsage(t, map[string]float64{"me@example.com": 94})
	if err := provider.SetAccountCap("codex", "me@example.com", 95); err != nil {
		t.Fatal(err)
	}
	if err := provider.SetCapCareful("codex", true); err != nil {
		t.Fatal(err)
	}
	read := standInReads(t)
	read.set("me@example.com", time.Now(), true)
	calls, post := compactCapGateway(t)
	if code, body := post(compactCapBody, "chatgpt-token", "acct-1"); code != 429 || calls.Load() != 0 || !strings.Contains(body, "usage couldn't be read") {
		t.Fatalf("usage unknown: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
	if asked := read.askedFor(); len(asked) != 1 || asked[0] != "me@example.com" {
		t.Fatalf("usage reread: %v", asked)
	}
	read.set("me@example.com", time.Now(), false)
	if code, body := post(compactCapBody, "chatgpt-token", "acct-1"); code != 200 || calls.Load() != 1 {
		t.Fatalf("usage known: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
}

func TestCodexNativeCompactNoCredits(t *testing.T) {
	codexSignedIn(t)
	capUsage(t, map[string]float64{"me@example.com": 100})
	if err := provider.SetCodexCredits("me@example.com", false); err != nil {
		t.Fatal(err)
	}
	calls, post := compactCapGateway(t)
	if code, body := post(compactCapBody, "chatgpt-token", "acct-1"); code != 429 || calls.Load() != 0 || !strings.Contains(body, "not to spend its credits") {
		t.Fatalf("no credits: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
}

func TestCodexNativeCompactOffProviderRelays(t *testing.T) {
	codexSignedIn(t)
	capUsage(t, map[string]float64{"me@example.com": 95})
	if err := provider.SetAccountCap("codex", "me@example.com", 95); err != nil {
		t.Fatal(err)
	}
	if err := provider.SetOff("codex", true); err != nil {
		t.Fatal(err)
	}
	calls, post := compactCapGateway(t)
	if code, body := post(compactCapBody, "chatgpt-token", "acct-1"); code != 200 || calls.Load() != 1 {
		t.Fatalf("provider off: status %d, upstream calls %d: %s", code, calls.Load(), body)
	}
}
