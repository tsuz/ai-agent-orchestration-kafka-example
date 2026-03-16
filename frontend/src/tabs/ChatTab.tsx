import { useState, useRef, useEffect, useCallback } from "react";
import Markdown from "react-markdown";
import type { ChatMessage, ToolSource } from "../types";
import "./ChatTab.css";

function SourceBlock({ source }: { source: ToolSource }) {
  const [expanded, setExpanded] = useState(true);
  const toggle = useCallback(() => setExpanded((p) => !p), []);

  return (
    <div className="source-block">
      <button className="source-header" onClick={toggle}>
        <span className="source-chevron">{expanded ? "▼" : "▶"}</span>
        <span className="source-name">{source.toolName}</span>
        <span className="source-label">raw output</span>
      </button>
      {expanded && (
        <pre className="source-console">{source.consoleOutput}</pre>
      )}
    </div>
  );
}

interface Props {
  messages: ChatMessage[];
  thinking: boolean;
  sessionId: string;
  onSend: (content: string) => void;
  onNewChat: () => void;
}

export function ChatTab({ messages, thinking, sessionId, onSend, onNewChat }: Props) {
  const [input, setInput] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, thinking]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed) return;
    onSend(trimmed);
    setInput("");
  };

  return (
    <div className="chat-tab">
      <div className="chat-header">
        <span className="chat-session-id">{sessionId}</span>
        <button className="chat-new-btn" onClick={onNewChat}>New Chat</button>
      </div>

      <div className="chat-messages">
        {messages.length === 0 && !thinking && (
          <div className="chat-empty">
            <p>Send a message to start a conversation with the AI agent.</p>
          </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`chat-bubble ${msg.role}`}>
            <div className="chat-role">{msg.role === "user" ? "You" : "Agent"}</div>
            <div className="chat-content">
              {msg.role === "assistant" ? <Markdown>{msg.content}</Markdown> : msg.content}
            </div>
            {msg.sources && msg.sources.length > 0 && (
              <div className="chat-sources">
                <div className="chat-sources-divider" />
                {msg.sources.map((src, i) => (
                  <SourceBlock key={i} source={src} />
                ))}
              </div>
            )}
            <div className="chat-time">
              {new Date(msg.timestamp).toLocaleTimeString()}
            </div>
          </div>
        ))}
        {thinking && (
          <div className="chat-bubble assistant">
            <div className="chat-role">Agent</div>
            <div className="chat-thinking">
              <span className="chat-thinking-dot" />
              <span className="chat-thinking-dot" />
              <span className="chat-thinking-dot" />
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <form className="chat-input-bar" onSubmit={handleSubmit}>
        <input
          className="chat-input"
          type="text"
          placeholder="Type a message..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          autoFocus
        />
        <button className="chat-send-btn" type="submit" disabled={!input.trim()}>
          Send
        </button>
      </form>
    </div>
  );
}
