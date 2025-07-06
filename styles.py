# styles.py


STYLES = """
<style>
#MainMenu, footer, header {visibility:hidden;}
.chat-container {
  display: flex;
  flex-direction: column-reverse;
  background: #fafafa;
  border: 1px solid #e6e9ef;
  border-radius: 8px;
  padding: 1rem;
  max-height: 550px;
  overflow-y: auto;
  max-width: 800px;
  margin: 0 auto;
}

.agent-message {background:#f0f0f0;padding:0.7rem 1rem;border-radius:1rem;
  margin:0.4rem 0 0.4rem 25%;text-align:left;}
.contact-message {background:#2196f3;color:#fff;padding:0.7rem 1rem;border-radius:1rem;
  margin:0.4rem 25% 0.4rem 0;text-align:left;}
.timestamp {font-size:0.7rem;color:#999;margin-top:0.25rem;text-align:right;}
.contact-message .timestamp {color:#e3f2fd;text-align:left;}
.reason-box {background:#fff3cd;border:1px solid #ffc107;border-radius:0.5rem;
  padding:1rem;color:#856404;}
.family-grid {display:flex;flex-direction:column;gap:8px;}
.family-card {background:#fff;padding:8px 12px;border:1px solid #e6e9ef;
  border-radius:6px;font-size:0.85rem;line-height:1.3;}
</style>
"""