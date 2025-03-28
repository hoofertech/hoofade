class MessageFeed {
  constructor() {
    this.loading = false;

    this.inProgressContainer = document.getElementById('in-progress-container');
    this.messagesContainer = document.getElementById('messages');
    this.loadingElement = document.getElementById('loading');
    this.messageTypeSelect = document.getElementById('messageType');
    this.newMessagesButton = document.getElementById('newMessages');
    this.newMessagesCountElement = document.getElementById('newMessagesCount');

    // Initialize state from URL parameters
    const params = new URLSearchParams(window.location.search);
    this.messageType = params.get('type') || 'all';
    this.granularity = params.get('granularity') || '15m';

    // Set initial UI state
    this.initializeUIState();
    this.setupMessageTypeSelect();
    this.setupGranularityControls();
    this.setupEventListeners();
    this.loadMessages(true);
    this.startPollingNewMessages();
  }

  initializeUIState() {
    this.pullStartY = 0;
    this.pullMoveY = 0;
    this.isPulling = false;
    this.pullThreshold = 80; // pixels to pull before refreshing
    this.lastScrollTop = 0;
    this.lastWheelCheck = 0;  // timestamp for last wheel check
    this.wheelThrottleMs = 1000;  // One second throttle

    // Set message type dropdown
    this.messageTypeSelect.value = this.messageType;

    // Set granularity buttons
    document.querySelectorAll('.granularity-btn').forEach(button => {
      if (button.dataset.granularity === this.granularity) {
        button.classList.add('active');
      } else {
        button.classList.remove('active');
      }
    });

    // Update URL if parameters are missing
    if (!window.location.search) {
      this.updateURL(this.messageType, this.granularity);
    }
  }

  setupEventListeners() {
    this.messageTypeSelect.addEventListener('change', () => {
      this.messageType = this.messageTypeSelect.value;
      this.loadMessages(true);
    });

    window.addEventListener('scroll', () => {
      if (this.shouldLoadMore()) {
        this.loadMessages(false);
      }
    });

    this.newMessagesButton.addEventListener('click', () => {
      this.loadNewMessages();
    });

    // wheel event listener for mouse wheel/touchpad scrolling
    window.addEventListener('wheel', (e) => this.handleWheel(e), { passive: true });

    document.addEventListener('touchstart', (e) => this.handlePullStart(e), { passive: true });
    document.addEventListener('touchmove', (e) => this.handlePullMove(e), { passive: true });
    document.addEventListener('touchend', () => this.handlePullEnd());

    document.addEventListener('mousedown', (e) => this.handlePullStart(e));
    document.addEventListener('mousemove', (e) => this.handlePullMove(e));
    document.addEventListener('mouseup', () => this.handlePullEnd());
  }

  setupMessageTypeSelect() {
    this.messageTypeSelect.addEventListener('change', (e) => {
      this.messageType = e.target.value;
      this.updateURL(this.messageType, this.granularity);
    });
  }

  setupGranularityControls() {
    const buttons = document.querySelectorAll('.granularity-btn');

    buttons.forEach(button => {
      button.addEventListener('click', (e) => {
        // Remove active class from all buttons
        buttons.forEach(btn => btn.classList.remove('active'));

        // Add active class to clicked button
        e.target.classList.add('active');

        // Update granularity and URL
        this.granularity = e.target.dataset.granularity;
        this.updateURL(this.messageType, this.granularity);
      });
    });
  }

  updateURL(type, granularity) {
    const params = new URLSearchParams(window.location.search);
    params.set('type', type);
    params.set('granularity', granularity);
    window.history.pushState({}, '', `${window.location.pathname}?${params.toString()}`);
    this.loadMessages(true);
  }

  shouldLoadMore() {
    if (!this.hasMoreMessages || this.loading) {
      return false;
    }
    const scrollPosition = window.innerHeight + window.scrollY;
    const scrollThreshold = document.documentElement.scrollHeight - 200;
    return scrollPosition >= scrollThreshold;
  }

  async loadMessages(fromScratch) {
    if (fromScratch) {
      this.messages = [];
      this.lastTimestamp = null;
      this.hasMoreMessages = true;
      this.firstLoadedTimestamp = null;
      this.newMessagesCount = 0;
      this.messagesContainer.innerHTML = '';
      this.newMessagesButton.classList.add('hidden');
      this.pendingNewMessages = [];
    }

    if (this.loading || !this.hasMoreMessages) return;

    this.loading = true;
    this.loadingElement.classList.remove('hidden');

    try {
      let url = `/api/messages?type=${this.messageType}&granularity=${this.granularity}&limit=20`;

      if (this.lastTimestamp) {
        url += `&before=${this.lastTimestamp}`;
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
      this.checkForNewMessages();
    }, 30000); // Check every 30 seconds
  }

  renderMessages(messages) {
    messages.forEach(message => {
      const messageElement = this.createMessageElement(message);
      this.messagesContainer.appendChild(messageElement);
    });
    this.updateInProgressMessage();
  }

  async updateInProgressMessage() {
    try {
      const response = await fetch(`/api/in-progress/${this.granularity}`);
      const data = await response.json();

      // Clear existing in-progress message
      this.inProgressContainer.innerHTML = '';

      if (data.message) {
        const messageElement = this.createMessageElement(data.message);
        this.inProgressContainer.appendChild(messageElement);
      }
    } catch (error) {
      console.error('Error updating in-progress message:', error);
    }
  }

  createMessageElement(message) {
    let isInProgress = message.metadata?.status === 'in_progress';
    const div = document.createElement('div');
    div.className = `message-card message ${isInProgress ? 'in-progress' : ''}`;

    // Extract timestamp from message content for trades
    let displayTimestamp = new Date(message.timestamp).toLocaleString();

    // Determine display message type
    let displayType = message.message_type;
    if (message.message_type === 'trd') {
      displayType = 'trade';
    } else if (message.message_type === 'pfl') {
      displayType = 'portfolio';
    }

    div.innerHTML = `
            <div class="message-header">
                <span class="message-type ${displayType.toLowerCase()}">${displayType}</span>
                <span class="timestamp message-timestamp">${displayTimestamp}</span>
                ${isInProgress ? '<span class="status-badge">Live Updates</span>' : ''}
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

  handlePullStart(e) {
    if (window.scrollY === 0) {
      this.isPulling = true;
      this.pullStartY = e.type === 'mousedown' ? e.pageY : e.touches[0].pageY;
      this.pullMoveY = this.pullStartY;
    }
  }

  handlePullMove(e) {
    if (!this.isPulling) return;

    this.pullMoveY = e.type === 'mousemove' ? e.pageY : e.touches[0].pageY;
    const pullDistance = this.pullMoveY - this.pullStartY;

    if (pullDistance > 0 && pullDistance < this.pullThreshold) {
      // Could add visual feedback here if desired
    }
  }

  handlePullEnd() {
    if (!this.isPulling) return;

    const pullDistance = this.pullMoveY - this.pullStartY;
    if (pullDistance > this.pullThreshold) {
      this.checkForNewMessages();
    }

    this.isPulling = false;
    this.pullStartY = 0;
    this.pullMoveY = 0;
  }

  async checkForNewMessages() {
    if (this.loading) return;

    try {
      let url = `/api/messages?type=${this.messageType}&granularity=${this.granularity}&limit=20`;
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

    this.updateInProgressMessage();
  }

  handleWheel(e) {
    // Check if we're at the top of the page
    if (window.scrollY === 0) {
      // If scrolling up (negative deltaY means scrolling up)
      if (e.deltaY < 0) {
        const now = Date.now();
        // Only proceed if enough time has passed since last check
        if (now - this.lastWheelCheck >= this.wheelThrottleMs) {
          this.lastWheelCheck = now;
          this.checkForNewMessages();
        }
      }
    }
  }
}

// Initialize the app
document.addEventListener('DOMContentLoaded', () => {
  new MessageFeed();
}); 
