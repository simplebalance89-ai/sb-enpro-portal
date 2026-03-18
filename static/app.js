/* ══════════════════════════════════════════════════════════════
   EnPro Filtration Mastermind — Frontend App
   ══════════════════════════════════════════════════════════════ */

(function () {
    'use strict';

    // ── Config ──
    const API_BASE = window.ENPRO_API_BASE || '';
    const SESSION_KEY = 'enpro_fm_session';
    const HISTORY_KEY = 'enpro_fm_history';

    // ── State ──
    let sessionId = localStorage.getItem(SESSION_KEY);
    if (!sessionId) {
        sessionId = crypto.randomUUID ? crypto.randomUUID() : uuidFallback();
        localStorage.setItem(SESSION_KEY, sessionId);
    }
    let lastFollowUps = [];   // Track numbered options
    let isLoading = false;

    // ── DOM refs ──
    const chatArea = document.getElementById('chatArea');
    const userInput = document.getElementById('userInput');
    const sendBtn = document.getElementById('sendBtn');
    const typingEl = document.getElementById('typingIndicator');
    const modalOverlay = document.getElementById('modalOverlay');
    const modalTitle = document.getElementById('modalTitle');
    const modalLabel = document.getElementById('modalLabel');
    const modalInput = document.getElementById('modalInput');
    const modalHint = document.getElementById('modalHint');

    let currentModalType = null;

    // ── UUID fallback ──
    function uuidFallback() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0;
            return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
    }

    // ── Time formatting ──
    function timeStr() {
        return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    // ── Auto-grow textarea ──
    window.autoGrow = function (el) {
        el.style.height = 'auto';
        el.style.height = Math.min(el.scrollHeight, 100) + 'px';
    };

    // ── Keyboard handling ──
    window.handleKeyDown = function (e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    // ── Send handler ──
    window.handleSend = function () {
        const text = userInput.value.trim();
        if (!text || isLoading) return;

        // Check for numbered option shortcut
        const num = parseInt(text);
        if (num >= 1 && num <= lastFollowUps.length && text === String(num)) {
            const resolved = lastFollowUps[num - 1];
            userInput.value = '';
            userInput.style.height = 'auto';
            sendMessage(resolved);
            return;
        }

        userInput.value = '';
        userInput.style.height = 'auto';
        sendMessage(text);
    };

    // ── Core: sendMessage ──
    window.sendMessage = async function (text) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', text);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Connection error. Please check your network and try again.');
            console.error('Chat error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Direct lookup ──
    window.doLookup = async function (partNumber) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', 'Lookup: ' + partNumber);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/lookup', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ part_number: partNumber, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Lookup failed. Please try again.');
            console.error('Lookup error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Search ──
    window.doSearch = async function (query) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', 'Search: ' + query);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/search', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Search failed. Please try again.');
            console.error('Search error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Chemical check ──
    window.doChemical = async function (chemical) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', 'Chemical compatibility: ' + chemical);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/chemical', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chemical: chemical, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Chemical check failed. Please try again.');
            console.error('Chemical error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Response handler ──
    function handleResponse(data) {
        if (!data) {
            appendMessage('bot', 'No response received.');
            return;
        }

        // Handle different response shapes
        if (data.products && Array.isArray(data.products)) {
            if (data.text) appendMessage('bot', formatMarkdown(data.text));
            data.products.forEach(function (p) {
                appendCard(renderProductCard(p));
                appendFollowUps(p.part_number || '');
            });
        } else if (data.chemical) {
            appendCard(renderChemicalCard(data.chemical));
        } else if (data.table) {
            appendCard(renderTableCard(data.table));
        } else if (data.product) {
            appendCard(renderProductCard(data.product));
            appendFollowUps(data.product.part_number || '');
        } else if (data.options && Array.isArray(data.options)) {
            if (data.text) appendMessage('bot', formatMarkdown(data.text));
            appendNumberedOptions(data.options);
        } else if (data.text || data.message || data.response) {
            const txt = data.text || data.message || data.response;
            appendMessage('bot', formatMarkdown(txt));

            // Check for embedded options
            if (data.follow_ups) {
                appendFollowUps('', data.follow_ups);
            }
        } else if (typeof data === 'string') {
            appendMessage('bot', formatMarkdown(data));
        } else {
            appendMessage('bot', formatMarkdown(JSON.stringify(data, null, 2)));
        }

        scrollToBottom();
    }

    // ── Render product card ──
    window.renderProductCard = function (p) {
        var html = '<div class="product-card">';
        html += '<div class="product-card-header">' + esc(p.part_number || 'Product') + '</div>';
        html += '<div class="product-card-body">';

        var fields = [
            ['Description', p.description],
            ['Extended Desc', p.extended_description],
            ['Product Type', p.product_type],
            ['Manufacturer', p.manufacturer]
        ];

        // Specs line
        var specs = [];
        if (p.micron) specs.push(p.micron + ' Micron');
        if (p.media) specs.push(p.media);
        if (p.temp_rating) specs.push(p.temp_rating);
        if (p.psi_rating) specs.push(p.psi_rating + ' PSI');
        if (p.flow_rate) specs.push(p.flow_rate);
        if (specs.length) fields.push(['Specs', specs.join(' | ')]);

        if (p.efficiency) fields.push(['Efficiency', p.efficiency]);

        fields.forEach(function (f) {
            if (f[1]) {
                html += '<div class="product-field">';
                html += '<div class="product-field-label">' + esc(f[0]) + '</div>';
                html += '<div class="product-field-value">' + esc(String(f[1])) + '</div>';
                html += '</div>';
            }
        });

        // Stock
        if (p.stock && typeof p.stock === 'object') {
            html += '<div class="stock-section">';
            html += '<div class="stock-title">Inventory</div>';
            var locations = Object.keys(p.stock);
            var hasStock = false;
            locations.forEach(function (loc) {
                var qty = p.stock[loc];
                if (qty > 0) {
                    hasStock = true;
                    var cls = qty >= 10 ? 'in-stock' : 'low-stock';
                    var badge = qty >= 10 ? 'green' : qty >= 3 ? 'orange' : 'red';
                    html += '<div class="stock-row ' + cls + '">';
                    html += '<span>' + esc(loc) + '</span>';
                    html += '<span class="stock-qty ' + badge + '">' + qty + ' in stock</span>';
                    html += '</div>';
                }
            });
            if (!hasStock) {
                html += '<div class="stock-row"><span style="color: var(--stock-red); font-weight: 600;">Out of stock at all locations</span></div>';
            }
            html += '</div>';
        }

        // Price
        if (p.price !== undefined && p.price !== null) {
            var priceVal = parseFloat(p.price);
            if (priceVal > 0) {
                html += '<div class="product-price has-price">$' + priceVal.toFixed(2) + '</div>';
            } else {
                html += '<div class="product-price no-price">Contact EnPro for pricing</div>';
            }
        }

        html += '</div>'; // body

        // Footer
        html += '<div class="product-footer">';
        html += 'For additional information: <strong>EnPro Inc</strong> &mdash; ';
        html += '<a href="mailto:service@enproinc.com" style="color: var(--accent);">service@enproinc.com</a>';
        html += ' | <a href="tel:18003232416" style="color: var(--accent);">1 (800) 323-2416</a>';
        html += '</div>';

        html += '</div>';
        return html;
    };

    // ── Render chemical card ──
    window.renderChemicalCard = function (data) {
        var html = '<div class="chemical-card">';
        html += '<div class="chemical-card-header">' + esc(data.chemical || 'Chemical Compatibility') + '</div>';
        html += '<div class="chemical-card-body">';

        if (data.compatibilities && Array.isArray(data.compatibilities)) {
            data.compatibilities.forEach(function (row) {
                var status = (row.status || '').toLowerCase();
                var rowCls = status === 'compatible' ? 'compatible' :
                    status === 'not compatible' ? 'not-compatible' : 'limited';
                var badgeCls = status === 'compatible' ? 'green' :
                    status === 'not compatible' ? 'red' : 'orange';

                html += '<div class="compat-row ' + rowCls + '">';
                html += '<span><strong>' + esc(row.material || row.media || '') + '</strong></span>';
                html += '<span class="compat-badge ' + badgeCls + '">' + esc(row.status || 'Unknown') + '</span>';
                html += '</div>';
            });
        }

        if (data.notes) {
            html += '<div style="margin-top: 10px; font-size: 12px; color: var(--text-light); font-style: italic;">' + esc(data.notes) + '</div>';
        }

        html += '</div></div>';
        return html;
    };

    // ── Render table card ──
    window.renderTableCard = function (data) {
        var html = '<div class="table-card">';
        if (data.title) {
            html += '<div class="table-card-header">' + esc(data.title) + '</div>';
        }
        html += '<div class="table-card-wrapper"><table>';

        if (data.headers && Array.isArray(data.headers)) {
            html += '<thead><tr>';
            data.headers.forEach(function (h) {
                html += '<th>' + esc(h) + '</th>';
            });
            html += '</tr></thead>';
        }

        if (data.rows && Array.isArray(data.rows)) {
            html += '<tbody>';
            data.rows.forEach(function (row) {
                html += '<tr>';
                if (Array.isArray(row)) {
                    row.forEach(function (cell) {
                        html += '<td>' + esc(String(cell)) + '</td>';
                    });
                } else if (typeof row === 'object') {
                    (data.headers || Object.keys(row)).forEach(function (k) {
                        html += '<td>' + esc(String(row[k] || '')) + '</td>';
                    });
                }
                html += '</tr>';
            });
            html += '</tbody>';
        }

        html += '</table></div></div>';
        return html;
    };

    // ── Follow-up buttons ──
    function appendFollowUps(partNumber, customFollowUps) {
        var followUps = customFollowUps || [
            'Chemical compatibility for ' + partNumber,
            'Similar specs to ' + partNumber,
            'Other manufacturers for ' + partNumber,
            'Quote ready for ' + partNumber
        ];

        var labels = customFollowUps ? customFollowUps : [
            'Chemical Compatibility',
            'Similar Specs',
            'Other Manufacturers',
            'Quote Ready'
        ];

        lastFollowUps = followUps;

        var container = document.createElement('div');
        container.className = 'followup-buttons';
        container.style.maxWidth = '85%';

        labels.forEach(function (label, i) {
            var btn = document.createElement('button');
            btn.className = 'followup-btn';
            btn.textContent = (i + 1) + '. ' + label;
            btn.onclick = function () { sendMessage(followUps[i]); };
            container.appendChild(btn);
        });

        chatArea.appendChild(container);
        scrollToBottom();
    }

    // ── Numbered options ──
    function appendNumberedOptions(options) {
        lastFollowUps = options;

        var wrapper = document.createElement('div');
        wrapper.className = 'msg bot';

        var bubble = document.createElement('div');
        bubble.className = 'msg-bubble';

        var optDiv = document.createElement('div');
        optDiv.className = 'numbered-options';

        options.forEach(function (opt, i) {
            var row = document.createElement('div');
            row.className = 'numbered-option';
            row.onclick = function () { sendMessage(opt); };
            row.innerHTML = '<span class="option-num">' + (i + 1) + '</span><span>' + esc(opt) + '</span>';
            optDiv.appendChild(row);
        });

        bubble.appendChild(optDiv);
        wrapper.appendChild(bubble);
        chatArea.appendChild(wrapper);
        scrollToBottom();
    }

    // ── Append message ──
    function appendMessage(role, html) {
        var wrapper = document.createElement('div');
        wrapper.className = 'msg ' + role;

        var bubble = document.createElement('div');
        bubble.className = 'msg-bubble';
        bubble.innerHTML = html;

        var time = document.createElement('div');
        time.className = 'msg-time';
        time.textContent = timeStr();

        wrapper.appendChild(bubble);
        wrapper.appendChild(time);
        chatArea.appendChild(wrapper);
        scrollToBottom();
    }

    // ── Append raw card HTML ──
    function appendCard(cardHtml) {
        var wrapper = document.createElement('div');
        wrapper.className = 'msg bot';
        wrapper.innerHTML = cardHtml;
        chatArea.appendChild(wrapper);
        scrollToBottom();
    }

    // ── Markdown-lite formatting ──
    function formatMarkdown(text) {
        if (!text) return '';
        var s = esc(text);
        // Bold
        s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        // Italic
        s = s.replace(/\*(.+?)\*/g, '<em>$1</em>');
        // Inline code
        s = s.replace(/`(.+?)`/g, '<code>$1</code>');
        // Line breaks
        s = s.replace(/\n/g, '<br>');
        return s;
    }

    // ── HTML escape ──
    function esc(str) {
        if (!str) return '';
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ── Loading state ──
    function setLoading(on) {
        isLoading = on;
        sendBtn.disabled = on;
        typingEl.classList.toggle('active', on);
        if (on) {
            chatArea.appendChild(typingEl);
            scrollToBottom();
        }
    }

    // ── Scroll ──
    function scrollToBottom() {
        requestAnimationFrame(function () {
            chatArea.scrollTop = chatArea.scrollHeight;
        });
    }

    // ── Clear welcome ──
    function clearWelcome() {
        var w = chatArea.querySelector('.welcome');
        if (w) w.remove();
    }

    // ── Modal management ──
    window.showModal = function (type) {
        currentModalType = type;
        modalOverlay.classList.add('active');

        switch (type) {
            case 'lookup':
                modalTitle.textContent = 'Lookup Part';
                modalLabel.textContent = 'Part Number';
                modalInput.placeholder = 'e.g., EPF-1234';
                modalHint.textContent = 'Enter the exact part number to look up.';
                break;
            case 'chemical':
                modalTitle.textContent = 'Chemical Compatibility';
                modalLabel.textContent = 'Chemical Name';
                modalInput.placeholder = 'e.g., Sulfuric Acid';
                modalHint.textContent = 'Enter the chemical to check media compatibility.';
                break;
            case 'search':
                modalTitle.textContent = 'Search Products';
                modalLabel.textContent = 'Search Query';
                modalInput.placeholder = 'e.g., 10 micron bag filter';
                modalHint.textContent = 'Describe what you need — specs, type, application.';
                break;
        }

        modalInput.value = '';
        setTimeout(function () { modalInput.focus(); }, 100);
    };

    window.hideModal = function (e) {
        if (e && e.target !== modalOverlay) return;
        modalOverlay.classList.remove('active');
        currentModalType = null;
    };

    window.modalSubmit = function () {
        var val = modalInput.value.trim();
        if (!val) return;

        hideModal();

        switch (currentModalType) {
            case 'lookup': doLookup(val); break;
            case 'chemical': doChemical(val); break;
            case 'search': doSearch(val); break;
        }
    };

    // ── Expose for inline onclick ──
    window.sendMessage = sendMessage;

})();
