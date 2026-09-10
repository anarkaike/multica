package agent

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// devinReaderDrainGrace bounds how long the turn waits for trailing ACP
// notifications after the session/prompt response.
var devinReaderDrainGrace = 2 * time.Second

// devinBlockedArgs are flags hardcoded by the daemon that must not be
// overridden by user-configured custom_args. `acp` is the protocol subcommand
// that drives the ACP JSON-RPC transport.
var devinBlockedArgs = map[string]blockedArgMode{
	"acp": blockedStandalone,
}

// devinBackend implements Backend by spawning `devin-orig acp` and communicating
// via the standard ACP JSON-RPC 2.0 transport over stdin/stdout.
//
// Devin CLI (the Cognition Agent Client Protocol server) advertises ACP v1,
// supports session/new, session/load, session/prompt and session/set_model,
// and reports usage in the same shape as Hermes/Kiro.
type devinBackend struct {
	cfg Config
}

func (b *devinBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "devin-orig"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("devin executable not found at %q: %w", execPath, err)
	}

	mcpServers, err := buildACPMcpServers(opts.McpConfig, b.cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("devin: invalid mcp_config: %w", err)
	}

	timeout := opts.Timeout
	runCtx, cancel := runContext(ctx, timeout)

	// Assemble argv. The daemon always launches Devin in ACP server mode.
	// Users may pass extra flags through custom_args; --model is honored
	// by Devin as the ACP default model.
	devinArgs := append([]string{"acp"}, filterCustomArgs(opts.CustomArgs, devinBlockedArgs, b.cfg.Logger)...)
	cmd := b.cfg.commandAt(execPath).exec(runCtx, devinArgs...)
	hideAgentWindow(cmd)
	b.cfg.logAgentCommand(cmd, newAgentCommandLogArgs(devinArgs, trustAgentCommandPositional(0, "acp")))

	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}

	// Devin reads its own config and credentials from standard locations,
	// but inherits the daemon's environment so MULTICA_DEVIN_* overrides
	// and task-specific variables are visible.
	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("devin stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("devin stdin pipe: %w", err)
	}

	providerErr := newACPProviderErrorSniffer("devin")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("devin stderr pipe: %w", err)
	}

	if err := startOwnedProcessTree(cmd, b.cfg.Logger); err != nil {
		cancel()
		return nil, fmt.Errorf("start devin: %w", err)
	}

	stderrSink := io.MultiWriter(newLogWriter(b.cfg.Logger, "[devin:stderr] "), providerErr)
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(stderrSink, stderr)
	}()

	b.cfg.Logger.Info("devin acp started", "pid", cmd.Process.Pid, "cwd", opts.Cwd, "model", opts.Model)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	var deliverable acpDeliverableTracker
	var streamingCurrentTurn atomic.Bool

	promptDone := make(chan hermesPromptResult, 1)
	activity := make(chan struct{}, 1)

	c := &hermesClient{
		cfg:     b.cfg,
		stdin:   stdin,
		pending: make(map[int]*pendingRPC),
		acceptNotification: func(string) bool {
			return streamingCurrentTurn.Load()
		},
		onActivity: func() {
			select {
			case activity <- struct{}{}:
			default:
			}
		},
		onMessage: func(msg Message) {
			if !streamingCurrentTurn.Load() {
				return
			}
			deliverable.observe(msg)
			trySend(msgCh, msg)
		},
		onPromptDone: func(result hermesPromptResult) {
			if !streamingCurrentTurn.Load() {
				return
			}
			select {
			case promptDone <- result:
			default:
			}
		},
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := newAgentStreamScanner(stdout)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			c.handleLine(line)
		}
		c.closeAllPending(fmt.Errorf("devin process exited"))
	}()

	go func() {
		defer close(msgCh)
		defer close(resCh)
		defer func() {
			stdin.Close()
			cancel()
			_ = cmd.Wait()
			releaseProcessGroup(cmd)
		}()

		startTime := time.Now()
		finalStatus := "completed"
		var finalError string
		var sessionID string
		var resumeRejected bool
		effectiveModel := strings.TrimSpace(opts.Model)

		// 1. Initialize handshake.
		initResult, err := c.request(runCtx, "initialize", map[string]any{
			"protocolVersion": 1,
			"clientInfo": map[string]any{
				"name":    "multica-agent-sdk",
				"version": "0.2.0",
			},
			"clientCapabilities": map[string]any{},
		})
		if err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("devin initialize failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}

		mcpServers = filterACPMcpServersByCapability(mcpServers, extractACPMcpCapabilities(initResult), "devin", b.cfg)

		// 2. Create or resume a session.
		cwd := opts.Cwd
		if cwd == "" {
			cwd = "."
		}

		if opts.ResumeSessionID != "" {
			result, err := c.request(runCtx, "session/load", map[string]any{
				"cwd":        cwd,
				"sessionId":  opts.ResumeSessionID,
				"mcpServers": mcpServers,
			})
			if err != nil {
				finalStatus, finalError, resumeRejected = classifyACPResumeFailure(
					runCtx, "devin", "session/load", err, timeout, b.cfg.Logger)
				resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds(), ResumeRejected: resumeRejected}
				return
			}
			sessionID = extractACPSessionID(result)
			if sessionID == "" {
				sessionID = opts.ResumeSessionID
			}
			if effectiveModel == "" {
				effectiveModel = extractACPCurrentModelID(result)
			}
		} else {
			result, err := c.request(runCtx, "session/new", map[string]any{
				"cwd":        cwd,
				"mcpServers": mcpServers,
			})
			if err != nil {
				finalStatus = "failed"
				finalError = fmt.Sprintf("devin session/new failed: %v", err)
				resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
				return
			}
			sessionID = extractACPSessionID(result)
			if sessionID == "" {
				finalStatus = "failed"
				finalError = "devin session/new returned no session ID"
				resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
				return
			}
			if effectiveModel == "" {
				effectiveModel = extractACPCurrentModelID(result)
			}
		}

		c.sessionID = sessionID
		b.cfg.Logger.Info("devin session created", "session_id", sessionID)

		// 3. Switch model if requested.
		if opts.Model != "" {
			if _, err := c.request(runCtx, "session/set_model", map[string]any{
				"sessionId": sessionID,
				"modelId":   opts.Model,
			}); err != nil {
				b.cfg.Logger.Warn("devin set_session_model failed", "error", err, "requested_model", opts.Model)
				finalStatus = "failed"
				finalError = fmt.Sprintf("devin could not switch to model %q: %v", opts.Model, err)
				if isACPSessionNotFound(err) {
					b.cfg.Logger.Warn("resumed session not found at set_model time; clearing session id", "session_id", sessionID)
					sessionID = ""
					resumeRejected = true
				}
				resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds(), SessionID: sessionID, ResumeRejected: resumeRejected}
				return
			}
			b.cfg.Logger.Info("devin session model set", "model", opts.Model)
		}

		// 4. Send prompt.
		trySend(msgCh, Message{Type: MessageStatus, Status: "running", SessionID: sessionID})
		streamingCurrentTurn.Store(true)

		promptBlocks := []map[string]any{{"type": "text", "text": prompt}}
		_, err = c.request(runCtx, "session/prompt", map[string]any{
			"sessionId": sessionID,
			"content":   promptBlocks,
			"prompt":    promptBlocks,
		})
		if err != nil {
			if runCtx.Err() == context.DeadlineExceeded {
				finalStatus = "timeout"
				finalError = fmt.Sprintf("devin timed out after %s", timeout)
			} else if runCtx.Err() == context.Canceled {
				finalStatus = "aborted"
				finalError = "execution cancelled"
			} else {
				finalStatus = "failed"
				finalError = fmt.Sprintf("devin session/prompt failed: %v", err)
				if opts.ResumeSessionID != "" && isACPSessionNotFound(err) {
					b.cfg.Logger.Warn("resumed session not found at prompt time; clearing session id", "session_id", sessionID)
					sessionID = ""
					resumeRejected = true
				}
			}
		} else {
			select {
			case pr := <-promptDone:
				if pr.stopReason == "cancelled" {
					finalStatus = "aborted"
					finalError = "devin cancelled the prompt"
				}
				c.mergeUsage(pr.usage)
			default:
			}
			waitForACPNotificationQuiescence(runCtx, activity, readerDone, acpNotificationQuietTime, devinReaderDrainGrace)
		}

		duration := time.Since(startTime)
		b.cfg.Logger.Info("devin finished", "pid", cmd.Process.Pid, "status", finalStatus, "duration", duration.Round(time.Millisecond).String())

		stdin.Close()
		if !waitForACPNotificationQuiescenceWithJoin(readerDone, stderrDone, devinReaderDrainGrace) {
			cancel()
			<-readerDone
			<-stderrDone
		}
		providerErr.Finalize()
		streamingCurrentTurn.Store(false)

		finalOutput, _ := deliverable.result()
		finalStatus, finalError = promoteACPResultOnProviderError(finalStatus, finalError, finalOutput, providerErr)

		u := c.accumulatedUsage()
		var usageMap map[string]TokenUsage
		if acpUsagePresent(u) {
			model := effectiveModel
			if model == "" {
				model = "unknown"
			}
			usageMap = map[string]TokenUsage{model: u}
		}

		resCh <- Result{
			Status:         finalStatus,
			Output:         finalOutput,
			Error:          finalError,
			DurationMs:     duration.Milliseconds(),
			SessionID:      sessionID,
			ResumeRejected: resumeRejected,
			Usage:          usageMap,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

// waitForACPNotificationQuiescenceWithJoin waits for either the reader or
// stderr copier to finish, or a grace period to elapse. It returns true when
// the reader finished on its own.
func waitForACPNotificationQuiescenceWithJoin(readerDone, stderrDone <-chan struct{}, grace time.Duration) bool {
	select {
	case <-readerDone:
		<-stderrDone
		return true
	case <-stderrDone:
		<-readerDone
		return true
	case <-time.After(grace):
		return false
	}
}

// discoverDevinModels enumerates the models Devin CLI reports as usable.
// Devin's ACP session/new does not currently expose a model catalog, so we
// parse `devin-orig models list` and fall back to a small static catalog if
// that fails or is unavailable.
func discoverDevinModels(ctx context.Context, runtimeCmd Command) ([]Model, error) {
	execPath := runtimeCmd.Path
	if execPath == "" {
		execPath = "devin-orig"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return devinStaticModels(), nil
	}

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := runtimeCmd.exec(runCtx, "models", "list")
	out, err := cmd.Output()
	if err != nil {
		return devinStaticModels(), nil
	}

	models, err := parseDevinModelsList(string(out))
	if err != nil || len(models) == 0 {
		return devinStaticModels(), nil
	}
	return models, nil
}

// parseDevinModelsList parses the human-readable output of `devin models list`.
// The format is:
//
//	Claude Opus 5 (claude-opus-5)
//	  swe-1-7                              SWE-1.7 Max  [...]
//	  swe-1-7-medium                       SWE-1.7 Medium  [...]
//
// Lines with at least two fields are treated as model entries.
func parseDevinModelsList(out string) ([]Model, error) {
	var models []Model
	seen := map[string]bool{}
	var currentProvider, currentFamily string

	familyRe := regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9\s\-\.()]*?)\s*\(([a-z0-9][a-z0-9_\-\.]*)\)\s*$`)

	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, " \t")
		if line == "" {
			continue
		}

		if m := familyRe.FindStringSubmatch(line); m != nil {
			currentFamily = m[2]
			currentProvider = inferDevinProvider(currentFamily)
			continue
		}

		// Model entry lines are indented.
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			continue
		}

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "aliases:") {
			continue
		}

		parts := strings.Fields(trimmed)
		if len(parts) < 2 {
			continue
		}

		modelID := parts[0]
		if seen[modelID] {
			continue
		}
		seen[modelID] = true

		// Heuristic label: take all tokens until a price/context marker.
		var labelParts []string
		for i := 1; i < len(parts); i++ {
			tok := parts[i]
			if strings.HasPrefix(tok, "[") || strings.Contains(tok, "/") {
				break
			}
			labelParts = append(labelParts, tok)
		}
		label := strings.Join(labelParts, " ")
		if label == "" {
			label = modelID
		}

		provider := currentProvider
		if provider == "" {
			provider = inferDevinProvider(modelID)
		}

		models = append(models, Model{ID: modelID, Label: label, Provider: provider})
	}

	return models, nil
}

func inferDevinProvider(id string) string {
	id = strings.ToLower(id)
	switch {
	case strings.HasPrefix(id, "claude"), strings.HasPrefix(id, "opus"), strings.HasPrefix(id, "fable"), strings.HasPrefix(id, "sonnet"):
		return "anthropic"
	case strings.HasPrefix(id, "gpt"), strings.HasPrefix(id, "o1"), strings.HasPrefix(id, "o3"):
		return "openai"
	case strings.HasPrefix(id, "gemini"):
		return "google"
	case strings.Contains(id, "swe"):
		return "cognition"
	default:
		return "cognition"
	}
}

func devinStaticModels() []Model {
	return []Model{
		{ID: "swe-1-7", Label: "SWE-1.7 Max", Provider: "cognition", Default: true},
		{ID: "swe-1-7-medium", Label: "SWE-1.7 Medium", Provider: "cognition"},
		{ID: "swe-1-7-lightning", Label: "SWE-1.7 Lightning Max", Provider: "cognition"},
		{ID: "swe-1-7-lightning-medium", Label: "SWE-1.7 Lightning Medium", Provider: "cognition"},
		{ID: "claude-sonnet-5", Label: "Claude Sonnet 5", Provider: "anthropic"},
		{ID: "claude-opus-5", Label: "Claude Opus 5", Provider: "anthropic"},
		{ID: "gpt-5-6-sol", Label: "GPT-5.6 Sol", Provider: "openai"},
		{ID: "gemini-3-8-flash", Label: "Gemini 3.8 Flash", Provider: "google"},
	}
}
