import type { ChatMessage } from "./ChatMessage";

export type AiGenerateRequest = {
  connectionId: string;
  prompt: string;
  currentQuery?: string | null;
  currentTable?: string | null;
  errorContext?: string | null;
  conversationHistory?: ChatMessage[] | null;
};
