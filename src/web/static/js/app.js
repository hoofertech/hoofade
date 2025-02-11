class MessageFeed {
  constructor() {
    this.messages = [];
    this.loading = false;
    this.lastTimestamp = null;
    this.messageType = 'all';
    this.hasMoreMessages = true;
    this.newMessagesCount = 0;
    this.firstLoadedTimestamp = null;
    this.pendingNewMessages = [];

    this.messagesContainer = document.getElementById('messages');
    this.loadingElement = document.getElementById('loading');
    this.messageTypeSelect = document.getElementById('messageType');
    this.newMessagesButton = document.getElementById('newMessages');
    this.newMessagesCountElement = document.getElementById('newMessagesCount');

    this.setupEventListeners();
    this.loadMessages();
    this.startPollingNewMessages();
  }

  setupEventListeners() {
    this.messageTypeSelect.addEventListener('change', () => {
      this.messageType = this.messageTypeSelect.value;
      this.messages = [];
      this.lastTimestamp = null;
      this.hasMoreMessages = true;
      this.firstLoadedTimestamp = null;
      this.newMessagesCount = 0;
      this.messagesContainer.innerHTML = '';
      this.newMessagesButton.classList.add('hidden');
      this.pendingNewMessages = [];
      this.loadMessages();
    });

    window.addEventListener('scroll', () => {
      if (this.shouldLoadMore()) {
        this.loadMessages();
      }
    });

    this.newMessagesButton.addEventListener('click', () => {
      this.loadNewMessages();
    });
  }

  shouldLoadMore() {
    if (!this.hasMoreMessages || this.loading) {
      return false;
    }
    const scrollPosition = window.innerHeight + window.scrollY;
    const scrollThreshold = document.documentElement.scrollHeight - 200;
    return scrollPosition >= scrollThreshold;
  }

  async loadMessages() {
    if (this.loading || !this.hasMoreMessages) return;

    this.loading = true;
    this.loadingElement.classList.remove('hidden');

    try {
      let url = '/api/messages?limit=20';
      if (this.lastTimestamp) {
        url += `&before=${this.lastTimestamp}`;
      }
      if (this.messageType !== 'all') {
        url += `&type=${this.messageType}`;
      }

      const response = await fetch(url);
      const data = await response.json();

      if (data.messages && data.messages.length > 0) {
        if (!this.firstLoadedTimestamp) {
          this.firstLoadedTimestamp = data.messages[0].timestamp;
        }
        this.messages.push(...data.messages);
        this.renderMessages(data.messages);
        this.lastTimestamp = data.messages[data.messages.length - 1].timestamp;
      } else {
        this.hasMoreMessages = false;
      }
    } catch (error) {
      console.error('Error loading messages:', error);
      this.hasMoreMessages = false;
    } finally {
      this.loading = false;
      this.loadingElement.classList.add('hidden');
    }
  }

  async loadNewMessages() {
    if (this.loading || !this.pendingNewMessages.length) return;

    this.loading = true;
    this.loadingElement.classList.remove('hidden');

    try {
      const fragment = document.createDocumentFragment();
      this.pendingNewMessages.forEach(message => {
        const messageElement = this.createMessageElement(message);
        fragment.appendChild(messageElement);
      });
      this.messagesContainer.insertBefore(fragment, this.messagesContainer.firstChild);

      // Scroll to the top of the page smoothly
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      });

      this.newMessagesCount = 0;
      this.pendingNewMessages = [];
      this.newMessagesButton.classList.add('hidden');
    } catch (error) {
      console.error('Error loading new messages:', error);
    } finally {
      this.loading = false;
      this.loadingElement.classList.add('hidden');
    }
  }

  startPollingNewMessages() {
    setInterval(async () => {
      if (this.loading) return;

      try {
        let url = '/api/messages?limit=20';
        if (this.firstLoadedTimestamp) {
          url += `&after=${this.firstLoadedTimestamp}`;
        }
        if (this.messageType !== 'all') {
          url += `&type=${this.messageType}`;
        }

        const response = await fetch(url);
        const data = await response.json();

        if (data.messages && data.messages.length > 0) {
          this.firstLoadedTimestamp = data.messages[0].timestamp;
          this.pendingNewMessages.unshift(...data.messages);
          this.newMessagesCount = this.pendingNewMessages.length;
          this.newMessagesCountElement.textContent = this.newMessagesCount;
          this.newMessagesButton.classList.remove('hidden');
        }
      } catch (error) {
        console.error('Error checking for new messages:', error);
      }
    }, 5000); // Check every 5 seconds
  }

  renderMessages(messages) {
    messages.forEach(message => {
      const messageElement = this.createMessageElement(message);
      this.messagesContainer.appendChild(messageElement);
    });
  }

  createMessageElement(message) {
    const div = document.createElement('div');
    div.className = 'message-card';

    // Extract timestamp from message content for trades
    let displayTimestamp;
    if (message.message_type === 'trade_batch') {
      // Extract date from first line of trade message (format: "Trades on DD MMM YYYY HH:MM")
      const match = message.content.match(/Trades on (\d{2} [A-Z]{3} \d{4} \d{2}:\d{2})/);
      if (match) {
        displayTimestamp = new Date(match[1]).toLocaleString();
      }
    } else if (message.message_type === 'portfolio') {
      // Extract date from first line of portfolio message (format: "Portfolio on DD MMM YYYY")
      const match = message.content.match(/Portfolio on (\d{2} [A-Z]{3} \d{4})/);
      if (match) {
        displayTimestamp = new Date(match[1]).toLocaleString();
      }
    }

    // Fallback to message timestamp if we couldn't extract from content
    if (!displayTimestamp) {
      displayTimestamp = new Date(message.timestamp).toLocaleString();
    }

    // Determine display message type
    let displayType = message.message_type;
    if (message.message_type === 'trade_batch') {
      displayType = 'trade';
    }

    div.innerHTML = `
            <div class="message-header">
                <span class="message-type ${displayType.toLowerCase()}">${displayType}</span>
                <span class="message-timestamp">${displayTimestamp}</span>
            </div>
            <div class="message-content">${this.formatContent(message.content)}</div>
        `;

    return div;
  }

  formatContent(content) {
    return content
      .replace(/\n/g, '<br>')
      .replace(/\$([A-Z]+)/g, '<strong>$$$1</strong>')
      .replace(/(BUY|SELL)/g, '<span class="trade-side">$1</span>');
  }
}

// Initialize the app
document.addEventListener('DOMContentLoaded', () => {
  new MessageFeed();
}); 
